package repository

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type WatchConfig struct {
	MaxRetries int
}

type WatchHandler struct {
	key          types.DbKey
	store        ResourceStore
	logger       *zap.Logger
	config       WatchConfig
	backoff      *lib.BackoffManager
	eventChan    chan<- types.WatchEvent
	clients      []chan<- types.WatchEvent
	clientsMutex sync.RWMutex
	lastRevision int64
	retryCount   int
}

func NewWatchHandler(
	key types.DbKey,
	store ResourceStore,
	logger *zap.Logger,
	config WatchConfig,
	backoff *lib.BackoffManager,
) *WatchHandler {
	return &WatchHandler{
		key:     key,
		store:   store,
		logger:  logger,
		config:  config,
		backoff: backoff,
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

func (h *WatchHandler) getClientCount() int {
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

func (h *WatchHandler) broadcastEvent(ctx context.Context, event types.WatchEvent) {
	clients := h.copyClients()

	for _, client := range clients {
		go func(client chan<- types.WatchEvent) {
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

		watchChan := make(chan types.WatchEvent, 100)

		// TODO: restart watcher with last revision if last error was not etcd's CompactedErr error
		// TODO: if last error was CompactedErr or if start of watcher with a specified revision causes CompactedErr we have to call reconciler process
		go h.store.Watch(watchCtx, h.key, watchChan)

		watchErr := h.processWatchEvents(watchCtx, watchChan)

		if watchErr == nil {
			h.backoff.Reset()
			h.retryCount = 0
			continue // watcher stopped without error
		}

		if h.retryCount >= h.config.MaxRetries {
			h.logger.Error("Max retries exceeded, stopping watcher",
				zap.String("key", h.key.ToKey()),
				zap.Int("retryCount", h.retryCount))
			return // todo: return specific error to notify caller
		}

		h.retryCount++
		backoffDuration := h.backoff.NextBackoff()

		h.logger.Warn("Watch error, retrying",
			zap.String("key", h.key.ToKey()),
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

func (h *WatchHandler) processWatchEvents(ctx context.Context, watchChan <-chan types.WatchEvent) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watchChan:
			if !ok {
				return errors.New("watch channel closed")
			}

			if event.Type == types.WatchEventTypeError {
				if errors.Is(event.Error, context.Canceled) {
					return nil
				}
				return event.Error
			}

			h.lastRevision = event.Revision
			h.broadcastEvent(ctx, event)
		}
	}
}

func (h *WatchHandler) reconcile(ctx context.Context) error {
	var resourceKey types.ResourceKey
	var err error

	switch key := h.key.(type) {
	case types.ResourceKey:
		resourceKey = key
	case types.ObjectKey:
		resourceKey = key.ToResourceKey()
	case types.GroupVersionKind:
		// For GVK, we need to list all namespaces, but this is complex
		// For now, we'll skip reconciliation for GVK-only keys
		return nil
	default:
		// Try to parse as ResourceKey string
		resourceKey, err = types.ParseResourceKey(h.key.ToKey())
		if err != nil {
			return err
		}
	}

	resources, err := h.store.List(ctx, resourceKey)
	if err != nil {
		return err
	}

	// TODO: handle the cache of resources revisions

	for _, resource := range resources {
		event := types.WatchEvent{
			Type:      types.WatchEventTypeAdded,
			Object:    resource,
			Timestamp: time.Now(),
		}
		// TODO: handle deletion events
		h.broadcastEvent(ctx, event)
	}

	return nil
}
