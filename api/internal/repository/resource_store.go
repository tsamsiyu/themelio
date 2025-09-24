package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type WatchEventType string

const (
	WatchEventTypeAdded    WatchEventType = "added"
	WatchEventTypeModified WatchEventType = "modified"
	WatchEventTypeDeleted  WatchEventType = "deleted"
	WatchEventTypeError    WatchEventType = "error"
)

type WatchEvent struct {
	Type      WatchEventType  `json:"type"`
	Object    *sdkmeta.Object `json:"object"`
	Timestamp time.Time       `json:"timestamp"`
	Revision  int64           `json:"revision,omitempty"`
	Error     error           `json:"error,omitempty"`
}

// ResourceStore provides resource-specific operations and watch functionality
type ResourceStore interface {
	// Basic CRUD operations with automatic marshaling
	Put(ctx context.Context, obj *sdkmeta.Object) error
	Get(ctx context.Context, key sdkmeta.ObjectKey) (*sdkmeta.Object, error)
	Delete(ctx context.Context, key sdkmeta.ObjectKey) error
	List(ctx context.Context, objType *sdkmeta.ObjectType, limit int) ([]*sdkmeta.Object, error)

	// Transaction operation builders
	BuildPutTxOp(obj *sdkmeta.Object) (clientv3.Op, error)

	// Watch operations
	Watch(ctx context.Context, objType *sdkmeta.ObjectType, eventChan chan<- WatchEvent) error
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
func (s *resourceStore) Put(ctx context.Context, obj *sdkmeta.Object) error {
	data, err := s.MarshalResource(obj)
	if err != nil {
		return err
	}

	key := objectKeyToDbKey(*obj.ObjectKey)
	return s.clientWrapper.Put(ctx, key, data)
}

func (s *resourceStore) Get(ctx context.Context, key sdkmeta.ObjectKey) (*sdkmeta.Object, error) {
	keyStr := objectKeyToDbKey(key)
	data, err := s.clientWrapper.Get(ctx, keyStr)
	if err != nil {
		return nil, err
	}
	return s.unmarshalResource(data)
}

func (s *resourceStore) Delete(ctx context.Context, key sdkmeta.ObjectKey) error {
	keyStr := objectKeyToDbKey(key)
	return s.clientWrapper.Delete(ctx, keyStr)
}

func (s *resourceStore) List(ctx context.Context, objType *sdkmeta.ObjectType, limit int) ([]*sdkmeta.Object, error) {
	keyStr := objectTypeToDbKey(objType)
	kvs, err := s.clientWrapper.List(ctx, keyStr, limit)
	if err != nil {
		return nil, err
	}

	var resources []*sdkmeta.Object
	for _, kv := range kvs {
		resource, err := s.unmarshalResource(kv.Value)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}

	return resources, nil
}

func (s *resourceStore) MarshalResource(resource *sdkmeta.Object) (string, error) {
	data, err := json.Marshal(resource)
	if err != nil {
		return "", internalerrors.NewMarshalingError("Failed to marshal resource")
	}
	return string(data), nil
}

func (s *resourceStore) BuildPutTxOp(resource *sdkmeta.Object) (clientv3.Op, error) {
	data, err := s.MarshalResource(resource)
	if err != nil {
		return clientv3.Op{}, err
	}
	key := objectKeyToDbKey(*resource.ObjectKey)
	return clientv3.OpPut(key, data), nil
}

func (s *resourceStore) Watch(ctx context.Context, objType *sdkmeta.ObjectType, eventChan chan<- WatchEvent) error {
	keyStr := objectTypeToDbKey(objType)
	go s.watchResources(ctx, keyStr, eventChan)
	return nil
}

// watchResources
// if eventChan is full this method will block until the channel is ready to receive the event
func (s *resourceStore) watchResources(ctx context.Context, prefix string, eventChan chan<- WatchEvent) {
	defer close(eventChan)

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	watchChan, err := s.clientWrapper.Watch(watchCtx, prefix)
	if err != nil {
		errorEvent := WatchEvent{
			Type:      WatchEventTypeError,
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
				errorEvent := WatchEvent{
					Type:      WatchEventTypeError,
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

func (s *resourceStore) convertEtcdEventToWatchEvent(ev *clientv3.Event, revision int64) (WatchEvent, error) {
	event := WatchEvent{
		Timestamp: time.Now(),
		Revision:  revision,
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
			objectKey, err := parseObjectKey(string(ev.Kv.Key))
			if err != nil {
				return event, errors.Wrap(err, "failed to parse etcd key")
			}

			event.Object = &sdkmeta.Object{
				ObjectKey: &sdkmeta.ObjectKey{
					ObjectType: sdkmeta.ObjectType{
						Group:     objectKey.Group,
						Version:   objectKey.Version,
						Kind:      objectKey.Kind,
						Namespace: objectKey.Namespace,
					},
					Name: objectKey.Name,
				},
			}
		}

	default:
		return event, errors.Errorf("unknown etcd event type: %v", ev.Type)
	}

	return event, nil
}

func (s *resourceStore) unmarshalResource(data []byte) (*sdkmeta.Object, error) {
	var obj sdkmeta.Object
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, internalerrors.NewMarshalingError("Failed to unmarshal resource")
	}

	return &obj, nil
}
