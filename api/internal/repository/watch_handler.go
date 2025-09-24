package repository

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type WatchConfig struct {
	MaxRetries int
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

		// TODO: restart watcher with last revision if last error was not etcd's CompactedErr error
		// TODO: if last error was CompactedErr or if start of watcher with a specified revision causes CompactedErr we have to call reconciler process
		go h.store.Watch(watchCtx, h.objType, watchChan)

		watchErr := h.processWatchEvents(watchCtx, watchChan)

		if watchErr == nil {
			h.backoff.Reset()
			h.retryCount = 0
			continue // watcher stopped without error
		}

		if h.retryCount >= h.config.MaxRetries {
			h.logger.Error("Max retries exceeded, stopping watcher",
				zap.String("key", objectTypeToDbKey(h.objType)),
				zap.Int("retryCount", h.retryCount))
			return // todo: return specific error to notify caller
		}

		h.retryCount++
		backoffDuration := h.backoff.NextBackoff()

		h.logger.Warn("Watch error, retrying",
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
				return errors.New("watch channel closed")
			}

			if event.Type == WatchEventTypeError {
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
	resources, err := h.store.List(ctx, h.objType, 0)
	if err != nil {
		return err
	}

	// TODO: handle the cache of resources revisions

	for _, resource := range resources {
		event := WatchEvent{
			Type:      WatchEventTypeAdded,
			Object:    resource,
			Timestamp: time.Now(),
		}
		// TODO: handle deletion events
		h.broadcastEvent(ctx, event)
	}

	return nil
}
