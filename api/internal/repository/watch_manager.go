package repository

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type WatchManager struct {
	store      ResourceStore
	logger     *zap.Logger
	config     WatchConfig
	backoff    *lib.BackoffManager
	handlers   map[string]*WatchHandler
	handlersMu sync.RWMutex
}

func NewWatchManager(store ResourceStore, logger *zap.Logger, config WatchConfig, backoff *lib.BackoffManager) *WatchManager {
	return &WatchManager{
		store:    store,
		logger:   logger,
		config:   config,
		backoff:  backoff,
		handlers: make(map[string]*WatchHandler),
	}
}

func (m *WatchManager) Watch(ctx context.Context, key types.ResourceKey) <-chan types.WatchEvent {
	clientChan := make(chan types.WatchEvent, 100)
	keyStr := key.ToKey()

	m.handlersMu.Lock()
	defer m.handlersMu.Unlock()

	handler, exists := m.handlers[keyStr]
	if !exists {
		handler = NewWatchHandler(key, m.store, m.logger, m.config, m.backoff)
		m.handlers[keyStr] = handler
		go m.startHandler(ctx, handler)
	}

	handler.AddClient(clientChan)

	go m.cleanupHandler(ctx, keyStr, handler, clientChan)

	return clientChan
}

func (m *WatchManager) startHandler(ctx context.Context, handler *WatchHandler) {
	eventChan := make(chan types.WatchEvent, 100)
	handler.Start(ctx, eventChan)

	go func() {
		for event := range eventChan {
			if event.Type == types.WatchEventTypeError {
				m.logger.Error("Handler error",
					zap.String("key", handler.key.ToKey()),
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
