package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
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
	Replace(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string, resource *unstructured.Unstructured) error
	Get(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error)
	List(ctx context.Context, gvk schema.GroupVersionKind, namespace string) ([]*unstructured.Unstructured, error)
	Delete(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error
	Watch(ctx context.Context, gvk schema.GroupVersionKind, namespace string, eventChan chan<- WatchEvent) error
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

func (r *resourceRepository) Replace(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string, resource *unstructured.Unstructured) error {
	key := r.buildKey(gvk, namespace, name)

	data, err := r.marshalResource(resource)
	if err != nil {
		return err
	}

	_, err = r.client.Put(ctx, key, data)
	if err != nil {
		return errors.Wrap(err, "failed to store resource in etcd")
	}

	r.logger.Debug("Resource stored successfully in etcd",
		zap.String("key", key),
		zap.String("group", gvk.Group),
		zap.String("version", gvk.Version),
		zap.String("kind", gvk.Kind),
		zap.String("namespace", namespace),
		zap.String("name", name))

	return nil
}

func (r *resourceRepository) Get(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error) {
	key := r.buildKey(gvk, namespace, name)

	resp, err := r.client.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get resource from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, NewNotFoundError(gvk.Kind, namespace, name)
	}

	resource, err := r.unmarshalResource(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (r *resourceRepository) List(ctx context.Context, gvk schema.GroupVersionKind, namespace string) ([]*unstructured.Unstructured, error) {
	prefix := r.buildKey(gvk, namespace, "")

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

func (r *resourceRepository) Delete(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error {
	key := r.buildKey(gvk, namespace, name)

	resp, err := r.client.Delete(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to delete resource from etcd")
	}

	if resp.Deleted == 0 {
		return NewNotFoundError(gvk.Kind, namespace, name)
	}

	r.logger.Debug("Resource deleted successfully from etcd",
		zap.String("key", key),
		zap.String("group", gvk.Group),
		zap.String("version", gvk.Version),
		zap.String("kind", gvk.Kind),
		zap.String("name", name),
		zap.String("namespace", namespace))

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

func (r *resourceRepository) Watch(ctx context.Context, gvk schema.GroupVersionKind, namespace string, eventChan chan<- WatchEvent) error {
	prefix := r.buildKey(gvk, namespace, "")

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
			event.Object = &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name":      r.extractNameFromKey(string(ev.Kv.Key)),
						"namespace": r.extractNamespaceFromKey(string(ev.Kv.Key)),
					},
				},
			}
		}

	default:
		return event, errors.Errorf("unknown etcd event type: %v", ev.Type)
	}

	return event, nil
}

func (r *resourceRepository) buildKey(gvk schema.GroupVersionKind, namespace, name string) string {
	if name == "" {
		if namespace == "" {
			return fmt.Sprintf("/%s/%s/%s/", gvk.Group, gvk.Version, gvk.Kind)
		}
		return fmt.Sprintf("/%s/%s/%s/%s/", gvk.Group, gvk.Version, gvk.Kind, namespace)
	}

	if namespace == "" {
		namespace = "default"
	}

	return fmt.Sprintf("/%s/%s/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind, namespace, name)
}

func (r *resourceRepository) extractNameFromKey(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) >= 5 {
		return parts[len(parts)-1]
	}
	return ""
}

func (r *resourceRepository) extractNamespaceFromKey(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) >= 5 {
		return parts[len(parts)-2]
	}
	return "default"
}
