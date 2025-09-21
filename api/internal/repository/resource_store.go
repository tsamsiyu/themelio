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

// ResourceStore provides resource-specific operations and watch functionality
type ResourceStore interface {
	// Basic CRUD operations with automatic marshaling
	Put(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error
	Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error)
	Delete(ctx context.Context, key types.ObjectKey) error
	List(ctx context.Context, key types.ResourceKey, limit int) ([]*unstructured.Unstructured, error)

	// Transaction operation builders
	BuildPutTxOp(key types.ObjectKey, resource *unstructured.Unstructured) (clientv3.Op, error)

	// Watch operations
	Watch(ctx context.Context, key types.DbKey, eventChan chan<- types.WatchEvent) error
}

type resourceStore struct {
	clientWrapper ClientWrapper
	logger        *zap.Logger
}

func NewResourceStore(logger *zap.Logger, clientWrapper ClientWrapper) ResourceStore {
	return &resourceStore{
		clientWrapper: clientWrapper,
		logger:        logger,
	}
}

// ResourceStore implementation
func (s *resourceStore) Put(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error {
	data, err := s.MarshalResource(resource)
	if err != nil {
		return err
	}
	return s.clientWrapper.Put(ctx, key.String(), data)
}

func (s *resourceStore) Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error) {
	data, err := s.clientWrapper.Get(ctx, key.String())
	if err != nil {
		return nil, err
	}
	return s.unmarshalResource(data)
}

func (s *resourceStore) Delete(ctx context.Context, key types.ObjectKey) error {
	return s.clientWrapper.Delete(ctx, key.String())
}

func (s *resourceStore) List(ctx context.Context, key types.ResourceKey, limit int) ([]*unstructured.Unstructured, error) {
	kvs, err := s.clientWrapper.List(ctx, key.String(), limit)
	if err != nil {
		return nil, err
	}

	var resources []*unstructured.Unstructured
	for _, kv := range kvs {
		resource, err := s.unmarshalResource(kv.Value)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}

	return resources, nil
}

func (s *resourceStore) MarshalResource(resource *unstructured.Unstructured) (string, error) {
	data, err := json.Marshal(resource)
	if err != nil {
		return "", internalerrors.NewMarshalingError("Failed to marshal resource")
	}
	return string(data), nil
}

func (s *resourceStore) BuildPutTxOp(key types.ObjectKey, resource *unstructured.Unstructured) (clientv3.Op, error) {
	data, err := s.MarshalResource(resource)
	if err != nil {
		return clientv3.Op{}, err
	}
	return clientv3.OpPut(key.String(), data), nil
}

func (s *resourceStore) Watch(ctx context.Context, key types.DbKey, eventChan chan<- types.WatchEvent) error {
	go s.watchResources(ctx, key.ToKey(), eventChan)
	return nil
}

// watchResources
// if eventChan is full this method will block until the channel is ready to receive the event
func (s *resourceStore) watchResources(ctx context.Context, prefix string, eventChan chan<- types.WatchEvent) {
	defer close(eventChan)

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	watchChan, err := s.clientWrapper.Watch(watchCtx, prefix)
	if err != nil {
		errorEvent := types.WatchEvent{
			Type:      types.WatchEventTypeError,
			Error:     err,
			Timestamp: time.Now(),
		}
		select {
		case eventChan <- errorEvent:
		case <-ctx.Done():
		}
		return
	}

	for {
		select {
		case <-ctx.Done():
			return

		case watchResp, ok := <-watchChan:
			if !ok {
				return
			}

			if watchResp.Err() != nil {
				errorEvent := types.WatchEvent{
					Type:      types.WatchEventTypeError,
					Error:     watchResp.Err(),
					Timestamp: time.Now(),
					Revision:  watchResp.CompactRevision,
				}
				select {
				case eventChan <- errorEvent:
					s.logger.Debug("Watch error event sent",
						zap.String("prefix", prefix),
						zap.String("type", string(errorEvent.Type)),
						zap.String("key", string(errorEvent.Error.Error())))
				case <-ctx.Done():
					break
				}
				return
			}

			for _, ev := range watchResp.Events {
				event, err := s.convertEtcdEventToWatchEvent(ev, watchResp.Header.Revision)
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
				}
			}
		}
	}
}

func (s *resourceStore) convertEtcdEventToWatchEvent(ev *clientv3.Event, revision int64) (types.WatchEvent, error) {
	event := types.WatchEvent{
		Timestamp: time.Now(),
		Revision:  revision,
	}

	switch ev.Type {
	case clientv3.EventTypePut:
		if ev.PrevKv == nil {
			event.Type = types.WatchEventTypeAdded
		} else {
			event.Type = types.WatchEventTypeModified
		}

		resource, err := s.unmarshalResource(ev.Kv.Value)
		if err != nil {
			return event, errors.Wrap(err, "failed to unmarshal resource from etcd event")
		}
		event.Object = resource

	case clientv3.EventTypeDelete:
		event.Type = types.WatchEventTypeDeleted

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

func (s *resourceStore) unmarshalResource(data []byte) (*unstructured.Unstructured, error) {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, internalerrors.NewMarshalingError("Failed to unmarshal resource")
	}

	return &obj, nil
}
