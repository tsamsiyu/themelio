package repository

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type WatchManager struct {
	store      ResourceStore
	logger     *zap.Logger
	handlers   map[string]*WatchHandler
	handlersMu sync.RWMutex
}

func NewWatchManager(store ResourceStore, logger *zap.Logger) *WatchManager {
	return &WatchManager{
		store:    store,
		logger:   logger,
		handlers: make(map[string]*WatchHandler),
	}
}

func (m *WatchManager) Watch(ctx context.Context, key string) <-chan types.WatchEvent {
	clientChan := make(chan types.WatchEvent, 100)

	m.handlersMu.Lock()
	defer m.handlersMu.Unlock()

	handler, exists := m.handlers[key]
	if !exists {
		watchConfig := DefaultWatchConfig()
		backoffConfig := DefaultBackoffConfig()
		backoff := NewBackoffManager(backoffConfig)
		handler = NewWatchHandler(key, m.store, m.logger, watchConfig, backoff)
		m.handlers[key] = handler
		go m.startHandler(ctx, handler)
	}

	handler.AddClient(clientChan)

	go m.cleanupHandler(ctx, key, handler, clientChan)

	return clientChan
}

func (m *WatchManager) startHandler(ctx context.Context, handler *WatchHandler) {
	eventChan := make(chan types.WatchEvent, 100)
	handler.Start(ctx, eventChan)

	go func() {
		for event := range eventChan {
			if event.Type == types.WatchEventTypeError {
				m.logger.Error("Handler error",
					zap.String("key", handler.key),
					zap.Error(event.Error))
			}
		}
	}()
}

func (m *WatchManager) cleanupHandler(ctx context.Context, key string, handler *WatchHandler, clientChan chan types.WatchEvent) {
	<-ctx.Done()
	handler.RemoveClient(clientChan)
	close(clientChan)

	if handler.getClientCount() == 0 {
		m.handlersMu.Lock()
		defer m.handlersMu.Unlock()
		if h, exists := m.handlers[key]; exists && h == handler {
			delete(m.handlers, key)
		}
	}
}

func (m *WatchManager) WatchClusterResource(ctx context.Context, gvk types.GroupVersionKind) <-chan types.WatchEvent {
	return m.Watch(ctx, gvk.String())
}

func (m *WatchManager) WatchNamespacedResource(ctx context.Context, gvk types.GroupVersionKind, namespace string) <-chan types.WatchEvent {
	key := types.NewResourceKey(gvk.Group, gvk.Version, gvk.Kind, namespace).String()
	return m.Watch(ctx, key)
}

func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        3 * time.Second,
		BackoffMultiplier: 0.1,
		ResetAfter:        1 * time.Minute,
	}
}

func DefaultWatchConfig() WatchConfig {
	return WatchConfig{
		MaxRetries: 5,
	}
}
