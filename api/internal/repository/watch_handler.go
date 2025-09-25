package repository

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type WatchHandler struct {
	ObjType      *sdkmeta.ObjectType
	Store        types.ResourceStore
	Logger       *zap.Logger
	config       types.WatchConfig
	backoff      *lib.BackoffManager
	eventChan    chan<- types.WatchEvent
	clients      []chan<- types.WatchEvent
	clientsMutex sync.RWMutex
	LastRevision int64
	retryCount   int
	Cache        map[sdkmeta.ObjectKey]types.WatchCacheEntry
}

func NewWatchHandler(
	objType *sdkmeta.ObjectType,
	store types.ResourceStore,
	logger *zap.Logger,
	config types.WatchConfig,
	backoff *lib.BackoffManager,
) *WatchHandler {
	return &WatchHandler{
		ObjType: objType,
		Store:   store,
		Logger:  logger,
		config:  config,
		backoff: backoff,
		Cache:   make(map[sdkmeta.ObjectKey]types.WatchCacheEntry),
	}
}

func (h *WatchHandler) Start(ctx context.Context, eventChan chan<- types.WatchEvent) {
	h.eventChan = eventChan
	h.AddClient(eventChan)
	go h.watchLoop(ctx)
}

func (h *WatchHandler) AddClient(clientChan chan<- types.WatchEvent) {
	h.clientsMutex.Lock()
	defer h.clientsMutex.Unlock()
	h.clients = append(h.clients, clientChan)
}

func (h *WatchHandler) RemoveClient(clientChan chan<- types.WatchEvent) {
	h.clientsMutex.Lock()
	defer h.clientsMutex.Unlock()
	for i, client := range h.clients {
		if client == clientChan {
			h.clients = append(h.clients[:i], h.clients[i+1:]...)
			break
		}
	}
}

func (h *WatchHandler) GetClientCount() int {
	h.clientsMutex.RLock()
	defer h.clientsMutex.RUnlock()
	return len(h.clients)
}

func (h *WatchHandler) copyClients() []chan<- types.WatchEvent {
	h.clientsMutex.RLock()
	defer h.clientsMutex.RUnlock()

	clients := make([]chan<- types.WatchEvent, len(h.clients))
	copy(clients, h.clients)
	return clients
}

func (h *WatchHandler) BroadcastEvent(ctx context.Context, event types.WatchEvent) {
	clients := h.copyClients()

	for _, client := range clients {
		select {
		case client <- event:
		case <-ctx.Done():
			return
		}
	}
}

func (h *WatchHandler) watchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		watchCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		watchChan := make(chan types.WatchEvent, 100)

		go h.Store.Watch(watchCtx, h.ObjType, watchChan, h.LastRevision+1)

		watchErr := h.ProcessWatchEvents(watchCtx, watchChan)

		if watchErr == nil {
			h.backoff.Reset()
			h.retryCount = 0
			h.Logger.Info("Watch stopped without error",
				zap.String("key", objectTypeToDbKey(h.ObjType)),
				zap.Int64("revision", h.LastRevision))
			continue
		}

		if watchErr == rpctypes.ErrCompacted {
			h.Logger.Warn("Watch failed due to etcd compaction, performing reconciliation",
				zap.String("key", objectTypeToDbKey(h.ObjType)),
				zap.Error(watchErr))

			if err := h.Reconcile(ctx); err != nil {
				h.Logger.Error("Reconciliation failed",
					zap.String("key", objectTypeToDbKey(h.ObjType)),
					zap.Error(err))
			} else {
				h.Logger.Info("Reconciliation completed successfully",
					zap.String("key", objectTypeToDbKey(h.ObjType)))
			}
		}

		if h.retryCount >= h.config.MaxRetries {
			h.Logger.Error("Max retries exceeded, stopping watcher",
				zap.String("key", objectTypeToDbKey(h.ObjType)),
				zap.Int("retryCount", h.retryCount))
			return // todo: return specific error to notify caller
		}

		h.retryCount++
		backoffDuration := h.backoff.NextBackoff()

		h.Logger.Warn("Retrying watch",
			zap.String("key", objectTypeToDbKey(h.ObjType)),
			zap.Error(watchErr),
			zap.Int("retryCount", h.retryCount),
			zap.Duration("backoff", backoffDuration))

		select {
		case <-time.After(backoffDuration):
		case <-ctx.Done():
			return
		}
	}
}

func (h *WatchHandler) ProcessWatchEvents(ctx context.Context, watchChan <-chan types.WatchEvent) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watchChan:
			if !ok {
				h.Logger.Info("Watch channel closed, stopping watch loop",
					zap.String("key", objectTypeToDbKey(h.ObjType)),
					zap.Int64("revision", h.LastRevision))
				return nil
			}

			switch event.Type {
			case types.WatchEventTypeError:
				if errors.Is(event.Error, context.Canceled) {
					return nil
				}
				return event.Error
			case types.WatchEventTypeModified:
				h.Cache[*event.Object.ObjectKey] = types.WatchCacheEntry{
					Version:        event.Object.SystemMeta.Version,
					ModRevision:    event.Object.SystemMeta.ModRevision,
					CreateRevision: event.Object.SystemMeta.CreateRevision,
				}
			case types.WatchEventTypeAdded:
				h.Cache[*event.Object.ObjectKey] = types.WatchCacheEntry{
					Version:        event.Object.SystemMeta.Version,
					ModRevision:    event.Object.SystemMeta.ModRevision,
					CreateRevision: event.Object.SystemMeta.CreateRevision,
				}
			case types.WatchEventTypeDeleted:
				delete(h.Cache, *event.Object.ObjectKey)
			}

			h.LastRevision = event.Revision
			h.BroadcastEvent(ctx, event)
		}
	}
}

func (h *WatchHandler) Reconcile(ctx context.Context) error {
	lastKey := ""
	allCurrentKeys := make(map[sdkmeta.ObjectKey]bool)

	for {
		batch, err := h.Store.List(ctx, h.ObjType, &types.Paging{
			Prefix:  objectTypeToDbKey(h.ObjType),
			Limit:   h.config.ReconcileBatchSize,
			LastKey: lastKey,
		})
		if err != nil {
			return err
		}

		if len(batch.Objects) == 0 {
			break
		}

		for _, obj := range batch.Objects {
			key := *obj.ObjectKey
			allCurrentKeys[key] = true
			cachedEntry, exists := h.Cache[key]

			if !exists {
				event := types.WatchEvent{
					Type:      types.WatchEventTypeAdded,
					Object:    obj,
					ObjectKey: key,
					Timestamp: time.Now(),
					Revision:  batch.Revision,
				}
				h.BroadcastEvent(ctx, event)
			} else {
				if cachedEntry.CreateRevision != obj.SystemMeta.CreateRevision {
					deletedEvent := types.WatchEvent{
						Type:      types.WatchEventTypeDeleted,
						Object:    nil,
						ObjectKey: key,
						Timestamp: time.Now(),
						Revision:  batch.Revision,
					}
					h.BroadcastEvent(ctx, deletedEvent)

					addedEvent := types.WatchEvent{
						Type:      types.WatchEventTypeAdded,
						Object:    obj,
						ObjectKey: key,
						Timestamp: time.Now(),
						Revision:  batch.Revision,
					}
					h.BroadcastEvent(ctx, addedEvent)
				} else if cachedEntry.ModRevision != obj.SystemMeta.ModRevision {
					event := types.WatchEvent{
						Type:      types.WatchEventTypeModified,
						Object:    obj,
						ObjectKey: key,
						Timestamp: time.Now(),
						Revision:  batch.Revision,
					}
					h.BroadcastEvent(ctx, event)
				}
			}

			h.Cache[key] = types.WatchCacheEntry{
				ModRevision:    obj.SystemMeta.ModRevision,
				CreateRevision: obj.SystemMeta.CreateRevision,
				Version:        obj.SystemMeta.Version,
			}
		}

		h.LastRevision = batch.Revision

		if len(batch.Objects) < h.config.ReconcileBatchSize {
			break
		}

		lastObject := batch.Objects[len(batch.Objects)-1]
		lastKey = objectKeyToDbKey(*lastObject.ObjectKey)
	}

	// check for objects in cache that are not in any batch (deleted objects)
	keysToDelete := make([]sdkmeta.ObjectKey, 0)
	for cacheKey := range h.Cache {
		if !allCurrentKeys[cacheKey] {
			keysToDelete = append(keysToDelete, cacheKey)
		}
	}

	for _, keyToDelete := range keysToDelete {
		delete(h.Cache, keyToDelete)
		deletedEvent := types.WatchEvent{
			Type:      types.WatchEventTypeDeleted,
			Object:    nil,
			ObjectKey: keyToDelete,
			Timestamp: time.Now(),
			Revision:  h.LastRevision,
		}
		h.BroadcastEvent(ctx, deletedEvent)
	}

	return nil
}
