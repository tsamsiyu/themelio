package repository

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type WatchConfig struct {
	MaxRetries         int
	ReconcileBatchSize int
}

type watchCacheEntry struct {
	version        int64
	createRevision int64
	modRevision    int64
}

type WatchHandler struct {
	objType      *sdkmeta.ObjectType
	store        ResourceStore
	logger       *zap.Logger
	config       WatchConfig
	backoff      *lib.BackoffManager
	eventChan    chan<- WatchEvent
	clients      []chan<- WatchEvent
	clientsMutex sync.RWMutex
	lastRevision int64
	retryCount   int
	cache        map[sdkmeta.ObjectKey]watchCacheEntry
}

func NewWatchHandler(
	objType *sdkmeta.ObjectType,
	store ResourceStore,
	logger *zap.Logger,
	config WatchConfig,
	backoff *lib.BackoffManager,
) *WatchHandler {
	return &WatchHandler{
		objType: objType,
		store:   store,
		logger:  logger,
		config:  config,
		backoff: backoff,
		cache:   make(map[sdkmeta.ObjectKey]watchCacheEntry),
	}
}

func (h *WatchHandler) Start(ctx context.Context, eventChan chan<- WatchEvent) {
	h.eventChan = eventChan
	h.AddClient(eventChan)
	go h.watchLoop(ctx)
}

func (h *WatchHandler) AddClient(clientChan chan<- WatchEvent) {
	h.clientsMutex.Lock()
	defer h.clientsMutex.Unlock()
	h.clients = append(h.clients, clientChan)
}

func (h *WatchHandler) RemoveClient(clientChan chan<- WatchEvent) {
	h.clientsMutex.Lock()
	defer h.clientsMutex.Unlock()
	for i, client := range h.clients {
		if client == clientChan {
			h.clients = append(h.clients[:i], h.clients[i+1:]...)
			break
		}
	}
}

func (h *WatchHandler) getClientCount() int {
	h.clientsMutex.RLock()
	defer h.clientsMutex.RUnlock()
	return len(h.clients)
}

func (h *WatchHandler) copyClients() []chan<- WatchEvent {
	h.clientsMutex.RLock()
	defer h.clientsMutex.RUnlock()

	clients := make([]chan<- WatchEvent, len(h.clients))
	copy(clients, h.clients)
	return clients
}

func (h *WatchHandler) broadcastEvent(ctx context.Context, event WatchEvent) {
	clients := h.copyClients()

	for _, client := range clients {
		go func(client chan<- WatchEvent) {
			select {
			case client <- event:
			case <-ctx.Done():
				return
			}
		}(client)
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

		watchChan := make(chan WatchEvent, 100)

		go h.store.Watch(watchCtx, h.objType, watchChan, h.lastRevision+1)

		watchErr := h.processWatchEvents(watchCtx, watchChan)

		if watchErr == nil {
			h.backoff.Reset()
			h.retryCount = 0
			h.logger.Info("Watch stopped without error",
				zap.String("key", objectTypeToDbKey(h.objType)),
				zap.Int64("revision", h.lastRevision))
			continue
		}

		if watchErr == rpctypes.ErrCompacted {
			h.logger.Warn("Watch failed due to etcd compaction, performing reconciliation",
				zap.String("key", objectTypeToDbKey(h.objType)),
				zap.Error(watchErr))

			if err := h.reconcile(ctx); err != nil {
				h.logger.Error("Reconciliation failed",
					zap.String("key", objectTypeToDbKey(h.objType)),
					zap.Error(err))
			} else {
				h.logger.Info("Reconciliation completed successfully",
					zap.String("key", objectTypeToDbKey(h.objType)))
			}
		}

		if h.retryCount >= h.config.MaxRetries {
			h.logger.Error("Max retries exceeded, stopping watcher",
				zap.String("key", objectTypeToDbKey(h.objType)),
				zap.Int("retryCount", h.retryCount))
			return // todo: return specific error to notify caller
		}

		h.retryCount++
		backoffDuration := h.backoff.NextBackoff()

		h.logger.Warn("Retrying watch",
			zap.String("key", objectTypeToDbKey(h.objType)),
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

func (h *WatchHandler) processWatchEvents(ctx context.Context, watchChan <-chan WatchEvent) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watchChan:
			if !ok {
				h.logger.Info("Watch channel closed, stopping watch loop",
					zap.String("key", objectTypeToDbKey(h.objType)),
					zap.Int64("revision", h.lastRevision))
				return nil
			}

			switch event.Type {
			case WatchEventTypeError:
				if errors.Is(event.Error, context.Canceled) {
					return nil
				}
				return event.Error
			case WatchEventTypeModified:
				h.cache[*event.Object.ObjectKey] = watchCacheEntry{
					version:        event.Object.SystemMeta.Version,
					modRevision:    event.Object.SystemMeta.ModRevision,
					createRevision: event.Object.SystemMeta.CreateRevision,
				}
			case WatchEventTypeAdded:
				h.cache[*event.Object.ObjectKey] = watchCacheEntry{
					version:        event.Object.SystemMeta.Version,
					modRevision:    event.Object.SystemMeta.ModRevision,
					createRevision: event.Object.SystemMeta.CreateRevision,
				}
			case WatchEventTypeDeleted:
				delete(h.cache, *event.Object.ObjectKey)
			}

			h.lastRevision = event.Revision
			h.broadcastEvent(ctx, event)
		}
	}
}

func (h *WatchHandler) reconcile(ctx context.Context) error {
	lastKey := ""

	for {
		batch, err := h.store.List(ctx, h.objType, &Paging{
			Prefix:  objectTypeToDbKey(h.objType),
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
			cachedEntry, exists := h.cache[key]

			if !exists {
				event := WatchEvent{
					Type:      WatchEventTypeAdded,
					Object:    obj,
					Timestamp: time.Now(),
					Revision:  batch.Revision,
				}
				h.broadcastEvent(ctx, event)
			} else {

				if obj.SystemMeta.CreateRevision > h.lastRevision {
					// obj was deleted and then recreated
					deletedEvent := WatchEvent{
						Type:      WatchEventTypeDeleted,
						Object:    nil,
						ObjectKey: key,
						Timestamp: time.Now(),
						Revision:  batch.Revision,
					}
					h.broadcastEvent(ctx, deletedEvent)
					createdEvent := WatchEvent{
						Type:      WatchEventTypeAdded,
						Object:    obj,
						ObjectKey: key,
						Timestamp: time.Now(),
						Revision:  batch.Revision,
					}
					h.broadcastEvent(ctx, createdEvent)
				} else if cachedEntry.modRevision != obj.SystemMeta.ModRevision {
					event := WatchEvent{
						Type:      WatchEventTypeModified,
						Object:    obj,
						Timestamp: time.Now(),
						Revision:  batch.Revision,
					}
					h.broadcastEvent(ctx, event)
				}
			}

			h.cache[key] = watchCacheEntry{
				modRevision:    obj.SystemMeta.ModRevision,
				createRevision: obj.SystemMeta.CreateRevision,
				version:        obj.SystemMeta.Version,
			}

			if obj.SystemMeta.ModRevision > h.lastRevision {
				h.lastRevision = obj.SystemMeta.ModRevision
			}
		}

		for cacheKey := range h.cache {
			var matchingObj *sdkmeta.Object
			for i := range batch.Objects {
				if *batch.Objects[i].ObjectKey == cacheKey {
					matchingObj = batch.Objects[i]
					break
				}
			}
			if matchingObj == nil {
				delete(h.cache, cacheKey)
				deletedEvent := WatchEvent{
					Type:      WatchEventTypeDeleted,
					Object:    nil,
					ObjectKey: cacheKey,
					Timestamp: time.Now(),
					Revision:  batch.Revision,
				}
				h.broadcastEvent(ctx, deletedEvent)
			}
		}

		if len(batch.Objects) < h.config.ReconcileBatchSize {
			break
		}

		lastObject := batch.Objects[len(batch.Objects)-1]
		lastKey = objectKeyToDbKey(*lastObject.ObjectKey)
		h.lastRevision = batch.Revision
	}

	return nil
}
