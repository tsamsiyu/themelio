package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type WatchEventType string

const (
	WatchEventTypeAdded    WatchEventType = "added"
	WatchEventTypeModified WatchEventType = "modified"
	WatchEventTypeDeleted  WatchEventType = "deleted"
)

type WatchEvent struct {
	Type      WatchEventType             `json:"type"`
	Object    *unstructured.Unstructured `json:"object"`
	Timestamp time.Time                  `json:"timestamp"`
}

type ResourceRepository interface {
	Replace(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error
	Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error)
	List(ctx context.Context, key types.ResourceKey) ([]*unstructured.Unstructured, error)
	Delete(ctx context.Context, key types.ObjectKey) error
	Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- WatchEvent) error
	MarkDeleted(ctx context.Context, key types.ObjectKey) error
	ListDeletions(ctx context.Context) ([]types.ObjectKey, error)
}

type resourceRepository struct {
	logger *zap.Logger
	client *clientv3.Client
}

func NewResourceRepository(logger *zap.Logger, client *clientv3.Client) ResourceRepository {
	return &resourceRepository{
		logger: logger,
		client: client,
	}
}

func (r *resourceRepository) Replace(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error {
	etcdKey := key.String()

	data, err := r.marshalResource(resource)
	if err != nil {
		return err
	}

	_, err = r.client.Put(ctx, etcdKey, data)
	if err != nil {
		return errors.Wrap(err, "failed to store resource in etcd")
	}

	r.logger.Debug("Resource stored successfully in etcd",
		zap.Object("objectKey", key))

	return nil
}

func (r *resourceRepository) Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error) {
	etcdKey := key.String()

	resp, err := r.client.Get(ctx, etcdKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get resource from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, NewNotFoundError(key)
	}

	resource, err := r.unmarshalResource(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (r *resourceRepository) List(ctx context.Context, key types.ResourceKey) ([]*unstructured.Unstructured, error) {
	prefix := key.String()

	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var resources []*unstructured.Unstructured
	for _, kv := range resp.Kvs {
		resource, err := r.unmarshalResource(kv.Value)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}

	return resources, nil
}

func (r *resourceRepository) Delete(ctx context.Context, key types.ObjectKey) error {
	etcdKey := key.String()

	resp, err := r.client.Delete(ctx, etcdKey)
	if err != nil {
		return errors.Wrap(err, "failed to delete resource from etcd")
	}

	if resp.Deleted == 0 {
		return NewNotFoundError(key)
	}

	r.logger.Debug("Resource deleted successfully from etcd",
		zap.Object("objectKey", key))

	return nil
}

func (r *resourceRepository) marshalResource(resource *unstructured.Unstructured) (string, error) {
	data, err := json.Marshal(resource)
	if err != nil {
		return "", internalerrors.NewMarshalingError("Failed to marshal resource")
	}
	return string(data), nil
}

func (r *resourceRepository) unmarshalResource(data []byte) (*unstructured.Unstructured, error) {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, internalerrors.NewMarshalingError("Failed to unmarshal resource")
	}

	return &obj, nil
}

func (r *resourceRepository) Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- WatchEvent) error {
	prefix := key.String()

	go r.watchResources(ctx, prefix, eventChan)

	return nil
}

func (r *resourceRepository) watchResources(ctx context.Context, prefix string, eventChan chan<- WatchEvent) {
	defer close(eventChan)

	watchChan := r.client.Watch(ctx, prefix, clientv3.WithPrefix())

	for {
		select {
		case <-ctx.Done():
			return

		case watchResp, ok := <-watchChan:
			if !ok {
				return
			}

			if watchResp.Err() != nil {
				r.logger.Error("Watch error occurred",
					zap.String("prefix", prefix),
					zap.Error(watchResp.Err()))
				continue
			}

			for _, ev := range watchResp.Events {
				event, err := r.convertEtcdEventToWatchEvent(ev)
				if err != nil {
					r.logger.Error("Failed to convert etcd event to watch event",
						zap.String("prefix", prefix),
						zap.Error(err))
					continue
				}

				select {
				case eventChan <- event:
					r.logger.Debug("Watch event sent",
						zap.String("prefix", prefix),
						zap.String("type", string(event.Type)),
						zap.String("key", string(ev.Kv.Key)))
				case <-ctx.Done():
					return
				default:
					r.logger.Warn("Watch event channel full, dropping event",
						zap.String("prefix", prefix),
						zap.String("type", string(event.Type)))
				}
			}
		}
	}
}

func (r *resourceRepository) convertEtcdEventToWatchEvent(ev *clientv3.Event) (WatchEvent, error) {
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

		resource, err := r.unmarshalResource(ev.Kv.Value)
		if err != nil {
			return event, errors.Wrap(err, "failed to unmarshal resource from etcd event")
		}
		event.Object = resource

	case clientv3.EventTypeDelete:
		event.Type = WatchEventTypeDeleted

		if ev.PrevKv != nil {
			resource, err := r.unmarshalResource(ev.PrevKv.Value)
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

// MarkDeleted marks a resource for deletion by setting deletionTimestamp and adding to deletion collection
func (r *resourceRepository) MarkDeleted(ctx context.Context, key types.ObjectKey) error {
	etcdKey := key.String()
	deletionKey := fmt.Sprintf("/deletion%s", etcdKey)

	getResp, err := r.client.Get(ctx, etcdKey)
	if err != nil {
		return errors.Wrap(err, "failed to get resource for deletion marking")
	}

	if len(getResp.Kvs) == 0 {
		return NewNotFoundError(key)
	}

	resource, err := r.unmarshalResource(getResp.Kvs[0].Value)
	if err != nil {
		return internalerrors.NewMarshalingError("failed to unmarshal resource for deletion marking")
	}

	if deletionTimestamp := resource.GetDeletionTimestamp(); deletionTimestamp != nil {
		r.logger.Debug("Resource already marked for deletion",
			zap.Object("objectKey", key),
			zap.Time("deletionTimestamp", deletionTimestamp.Time))
		return nil // Already marked for deletion
	}

	now := v1.NewTime(time.Now())
	resource.SetDeletionTimestamp(&now)

	updatedData, err := r.marshalResource(resource)
	if err != nil {
		return internalerrors.NewMarshalingError("failed to marshal resource with deletion timestamp")
	}

	deletionRecord := map[string]interface{}{
		"objectKey": key,
		"timestamp": now.Time,
	}
	deletionData, err := json.Marshal(deletionRecord)
	if err != nil {
		return internalerrors.NewMarshalingError("failed to marshal deletion record")
	}

	txn := r.client.Txn(ctx)

	txnResp, err := txn.
		Then(clientv3.OpPut(etcdKey, updatedData)).
		Then(clientv3.OpPut(deletionKey, string(deletionData))).
		Commit()

	if err != nil {
		return errors.Wrap(err, "failed to execute deletion marking transaction")
	}

	if !txnResp.Succeeded {
		return errors.New("deletion marking transaction failed")
	}

	return nil
}

// ListDeletions returns all resources marked for deletion
func (r *resourceRepository) ListDeletions(ctx context.Context) ([]types.ObjectKey, error) {
	prefix := "/deletion/"

	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "failed to list deletion records from etcd")
	}

	var objectKeys []types.ObjectKey
	for _, kv := range resp.Kvs {
		var deletionRecord map[string]interface{}
		if err := json.Unmarshal(kv.Value, &deletionRecord); err != nil {
			continue
		}

		objectKeyRaw, ok := deletionRecord["objectKey"]
		if !ok {
			continue
		}

		objectKey, err := types.ParseObjectKey(objectKeyRaw.(string))
		if err != nil {
			continue
		}

		objectKeys = append(objectKeys, objectKey)
	}

	return objectKeys, nil
}
