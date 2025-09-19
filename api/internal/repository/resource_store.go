package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

// KeyValue represents a key-value pair from etcd
type KeyValue struct {
	Key   string
	Value []byte
}

// ResourceStore provides a thin wrapper over etcd client with marshaling/unmarshaling
type ResourceStore interface {
	// Basic CRUD operations with automatic marshaling
	Put(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error
	Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error)
	Delete(ctx context.Context, key types.ObjectKey) error
	List(ctx context.Context, key types.ResourceKey) ([]*unstructured.Unstructured, error)

	// Raw operations for complex transactions
	GetRaw(ctx context.Context, key string) ([]byte, error)
	ListRaw(ctx context.Context, prefix string) ([]KeyValue, error)

	// Transaction execution
	ExecuteTransaction(ctx context.Context, ops []clientv3.Op) error

	// Watch operations
	Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- WatchEvent) error
}

type resourceStore struct {
	logger *zap.Logger
	client *clientv3.Client
}

func NewResourceStore(logger *zap.Logger, client *clientv3.Client) ResourceStore {
	return &resourceStore{
		logger: logger,
		client: client,
	}
}

// ResourceStore implementation
func (s *resourceStore) Put(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error {
	data, err := s.marshalResource(resource)
	if err != nil {
		return err
	}
	_, err = s.client.Put(ctx, key.String(), data)
	return err
}

func (s *resourceStore) Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error) {
	resp, err := s.client.Get(ctx, key.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get resource from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, NewNotFoundError(key.String())
	}

	return s.unmarshalResource(resp.Kvs[0].Value)
}

func (s *resourceStore) Delete(ctx context.Context, key types.ObjectKey) error {
	_, err := s.client.Delete(ctx, key.String())
	return err
}

func (s *resourceStore) GetRaw(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get resource from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, NewNotFoundError(key)
	}

	return resp.Kvs[0].Value, nil
}

func (s *resourceStore) ListRaw(ctx context.Context, prefix string) ([]KeyValue, error) {
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "failed to list resources from etcd")
	}

	var kvs []KeyValue
	for _, kv := range resp.Kvs {
		kvs = append(kvs, KeyValue{
			Key:   string(kv.Key),
			Value: kv.Value,
		})
	}

	return kvs, nil
}

func (s *resourceStore) List(ctx context.Context, key types.ResourceKey) ([]*unstructured.Unstructured, error) {
	prefix := key.String()
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var resources []*unstructured.Unstructured
	for _, kv := range resp.Kvs {
		resource, err := s.unmarshalResource(kv.Value)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}

	return resources, nil
}

func (s *resourceStore) ExecuteTransaction(ctx context.Context, ops []clientv3.Op) error {
	txn := s.client.Txn(ctx)
	txnResp, err := txn.Then(ops...).Commit()
	if err != nil {
		return errors.Wrap(err, "failed to execute transaction")
	}

	if !txnResp.Succeeded {
		return errors.New("transaction failed")
	}

	return nil
}

func (s *resourceStore) Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- WatchEvent) error {
	go s.watchResources(ctx, key.String(), eventChan)
	return nil
}

func (s *resourceStore) marshalResource(resource *unstructured.Unstructured) (string, error) {
	data, err := json.Marshal(resource)
	if err != nil {
		return "", internalerrors.NewMarshalingError("Failed to marshal resource")
	}
	return string(data), nil
}

func (s *resourceStore) unmarshalResource(data []byte) (*unstructured.Unstructured, error) {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, internalerrors.NewMarshalingError("Failed to unmarshal resource")
	}

	return &obj, nil
}

func (s *resourceStore) watchResources(ctx context.Context, prefix string, eventChan chan<- WatchEvent) {
	defer close(eventChan)

	watchChan := s.client.Watch(ctx, prefix, clientv3.WithPrefix())

	for {
		select {
		case <-ctx.Done():
			return

		case watchResp, ok := <-watchChan:
			if !ok {
				return
			}

			if watchResp.Err() != nil {
				s.logger.Error("Watch error occurred",
					zap.String("prefix", prefix),
					zap.Error(watchResp.Err()))
				continue
			}

			for _, ev := range watchResp.Events {
				event, err := s.convertEtcdEventToWatchEvent(ev)
				if err != nil {
					s.logger.Error("Failed to convert etcd event to watch event",
						zap.String("prefix", prefix),
						zap.Error(err))
					continue
				}

				select {
				case eventChan <- event:
					s.logger.Debug("Watch event sent",
						zap.String("prefix", prefix),
						zap.String("type", string(event.Type)),
						zap.String("key", string(ev.Kv.Key)))
				case <-ctx.Done():
					return
				default:
					s.logger.Warn("Watch event channel full, dropping event",
						zap.String("prefix", prefix),
						zap.String("type", string(event.Type)))
				}
			}
		}
	}
}

func (s *resourceStore) convertEtcdEventToWatchEvent(ev *clientv3.Event) (WatchEvent, error) {
	event := WatchEvent{
		Timestamp: time.Now(),
	}

	switch ev.Type {
	case clientv3.EventTypePut:
		if ev.PrevKv == nil {
			event.Type = WatchEventTypeAdded
		} else {
			event.Type = WatchEventTypeModified
		}

		resource, err := s.unmarshalResource(ev.Kv.Value)
		if err != nil {
			return event, errors.Wrap(err, "failed to unmarshal resource from etcd event")
		}
		event.Object = resource

	case clientv3.EventTypeDelete:
		event.Type = WatchEventTypeDeleted

		if ev.PrevKv != nil {
			resource, err := s.unmarshalResource(ev.PrevKv.Value)
			if err != nil {
				return event, errors.Wrap(err, "failed to unmarshal deleted resource from etcd event")
			}
			event.Object = resource
		} else {
			objectKey, err := types.ParseObjectKey(string(ev.Kv.Key))
			if err != nil {
				return event, errors.Wrap(err, "failed to parse etcd key")
			}

			event.Object = &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name":      objectKey.Name,
						"namespace": objectKey.Namespace,
					},
				},
			}
		}

	default:
		return event, errors.Errorf("unknown etcd event type: %v", ev.Type)
	}

	return event, nil
}
