package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type resourceStore struct {
	clientWrapper types.ClientWrapper
	logger        *zap.Logger
}

func NewResourceStore(logger *zap.Logger, clientWrapper types.ClientWrapper) types.ResourceStore {
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
	kv, err := s.clientWrapper.Get(ctx, keyStr)
	if err != nil {
		return nil, err
	}
	return s.unmarshalResource(kv)
}

func (s *resourceStore) Delete(ctx context.Context, key sdkmeta.ObjectKey) error {
	keyStr := objectKeyToDbKey(key)
	return s.clientWrapper.Delete(ctx, keyStr)
}

func (s *resourceStore) List(ctx context.Context, objType *sdkmeta.ObjectType, paging *types.Paging) (*types.ObjectBatch, error) {
	if paging == nil {
		paging = &types.Paging{Prefix: objectTypeToDbKey(objType)}
	} else {
		paging.Prefix = objectTypeToDbKey(objType)
	}

	batch, err := s.clientWrapper.List(ctx, *paging)
	if err != nil {
		return nil, err
	}

	var objects []*sdkmeta.Object
	for _, kv := range batch.KVs {
		object, err := s.unmarshalResource(&kv)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}

	return &types.ObjectBatch{
		Revision: batch.Revision,
		Objects:  objects,
	}, nil
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

func (s *resourceStore) Watch(ctx context.Context, objType *sdkmeta.ObjectType, eventChan chan<- types.WatchEvent, revision ...int64) error {
	keyStr := objectTypeToDbKey(objType)

	// Use revision if provided, otherwise start from beginning
	var startRevision int64 = 0
	if len(revision) > 0 && revision[0] > 0 {
		startRevision = revision[0]
	}

	go s.watchResources(ctx, keyStr, eventChan, startRevision)
	return nil
}

// watchResources
// if eventChan is full this method will block until the channel is ready to receive the event
func (s *resourceStore) watchResources(ctx context.Context, prefix string, eventChan chan<- types.WatchEvent, revision int64) {
	defer close(eventChan)

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	watchChan, err := s.clientWrapper.Watch(watchCtx, prefix, revision)
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

		kv := convertClientKV(ev.Kv)
		resource, err := s.unmarshalResource(&kv)
		if err != nil {
			return event, errors.Wrap(err, "failed to unmarshal resource from etcd event")
		}
		event.Object = resource

	case clientv3.EventTypeDelete:
		event.Type = types.WatchEventTypeDeleted

		if ev.PrevKv != nil {
			kv := convertClientKV(ev.PrevKv)
			resource, err := s.unmarshalResource(&kv)
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

func (s *resourceStore) unmarshalResource(kv *types.KeyValue) (*sdkmeta.Object, error) {
	var obj sdkmeta.Object
	if err := json.Unmarshal(kv.Value, &obj); err != nil {
		return nil, internalerrors.NewMarshalingError("Failed to unmarshal resource")
	}

	obj.SystemMeta.Version = kv.Version
	obj.SystemMeta.ModRevision = kv.ModRevision
	obj.SystemMeta.CreateRevision = kv.CreateRevision

	return &obj, nil
}
