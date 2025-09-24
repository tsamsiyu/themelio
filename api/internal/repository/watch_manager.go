package repository

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
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

func (m *WatchManager) Watch(ctx context.Context, objType *sdkmeta.ObjectType, namespace string) <-chan WatchEvent {
	clientChan := make(chan WatchEvent, 100)
	keyStr := objectTypeToDbKey(objType)

	m.handlersMu.Lock()
	defer m.handlersMu.Unlock()

	handler, exists := m.handlers[keyStr]
	if !exists {
		handler = NewWatchHandler(objType, m.store, m.logger, m.config, m.backoff)
		m.handlers[keyStr] = handler
		go m.startHandler(ctx, handler)
	}

	handler.AddClient(clientChan)

	go m.cleanupHandler(ctx, keyStr, handler, clientChan)

	return clientChan
}

func (m *WatchManager) startHandler(ctx context.Context, handler *WatchHandler) {
	eventChan := make(chan WatchEvent, 100)
	handler.Start(ctx, eventChan)

	go func() {
		for event := range eventChan {
			if event.Type == WatchEventTypeError {
				m.logger.Error("Handler error",
					zap.String("key", objectTypeToDbKey(handler.objType)),
					zap.Error(event.Error))
			}
		}
	}()
}

func (m *WatchManager) cleanupHandler(ctx context.Context, key string, handler *WatchHandler, clientChan chan WatchEvent) {
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
