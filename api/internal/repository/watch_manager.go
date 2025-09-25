package repository

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

const (
	blockingClientEvictionTimeout = 100 * time.Millisecond
)

type WatchManager struct {
	store      types.ResourceStore
	logger     *zap.Logger
	config     types.WatchConfig
	backoff    *lib.BackoffManager
	handlers   map[string]*WatchHandler
	handlersMu sync.RWMutex
	clients    map[string][]chan<- types.WatchEvent
	clientsMu  sync.RWMutex
}

func NewWatchManager(
	store types.ResourceStore,
	logger *zap.Logger,
	config types.WatchConfig,
	backoff *lib.BackoffManager,
) *WatchManager {
	return &WatchManager{
		store:    store,
		logger:   logger,
		config:   config,
		backoff:  backoff,
		handlers: make(map[string]*WatchHandler),
		clients:  make(map[string][]chan<- types.WatchEvent),
	}
}

func (m *WatchManager) Watch(ctx context.Context, objType *sdkmeta.ObjectType) (<-chan types.WatchEvent, error) {
	clientChan := make(chan types.WatchEvent, 100)
	keyStr := objectTypeToDbKey(objType)

	m.handlersMu.Lock()
	defer m.handlersMu.Unlock()

	_, exists := m.handlers[keyStr]
	if !exists {
		handler := NewWatchHandler(objType, m.store, m.logger, m.config, m.backoff)
		m.handlers[keyStr] = handler
		go m.startHandler(ctx, keyStr, handler)
	}

	m.addClient(keyStr, clientChan)

	go m.cleanupHandler(ctx, keyStr, clientChan)

	return clientChan, nil
}

func (m *WatchManager) startHandler(ctx context.Context, key string, handler *WatchHandler) {
	handler.Start(ctx)

	go func() {
		eventChan := handler.EventChannel()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-eventChan:
				if !ok {
					m.logger.Warn("Handler event channel closed, cleaning up clients",
						zap.String("key", key))
					m.cleanupHandlerClients(key)
					return
				}

				if event.Type == types.WatchEventTypeError {
					m.logger.Error("Handler error",
						zap.String("key", key),
						zap.Error(event.Error))
				}

				m.broadcastEvent(ctx, key, event)
			}
		}
	}()
}

func (m *WatchManager) cleanupHandler(
	ctx context.Context,
	key string,
	clientChan chan types.WatchEvent,
) {
	<-ctx.Done()
	m.removeClient(key, clientChan)
	close(clientChan)

	if m.getClientCount(key) == 0 {
		m.handlersMu.Lock()
		defer m.handlersMu.Unlock()
		delete(m.handlers, key)
	}
}

func (m *WatchManager) cleanupHandlerClients(key string) {
	m.clientsMu.Lock()
	defer m.clientsMu.Unlock()

	clients := m.clients[key]
	for _, client := range clients {
		close(client)
	}

	delete(m.clients, key)

	m.handlersMu.Lock()
	defer m.handlersMu.Unlock()
	delete(m.handlers, key)
}

func (m *WatchManager) addClient(key string, clientChan chan<- types.WatchEvent) {
	m.clientsMu.Lock()
	defer m.clientsMu.Unlock()
	m.clients[key] = append(m.clients[key], clientChan)
}

func (m *WatchManager) removeClient(key string, clientChan chan<- types.WatchEvent) {
	m.clientsMu.Lock()
	defer m.clientsMu.Unlock()

	clients := m.clients[key]
	for i, client := range clients {
		if client == clientChan {
			m.clients[key] = append(clients[:i], clients[i+1:]...)
			break
		}
	}
}

func (m *WatchManager) getClientCount(key string) int {
	m.clientsMu.RLock()
	defer m.clientsMu.RUnlock()
	return len(m.clients[key])
}

func (m *WatchManager) copyClients(key string) []chan<- types.WatchEvent {
	m.clientsMu.RLock()
	defer m.clientsMu.RUnlock()

	clients := m.clients[key]
	result := make([]chan<- types.WatchEvent, len(clients))
	copy(result, clients)
	return result
}

func (m *WatchManager) broadcastEvent(ctx context.Context, key string, event types.WatchEvent) {
	clients := m.copyClients(key)

	for _, client := range clients {
		select {
		case client <- event:
			continue
		case <-ctx.Done():
			return
		default:
			// Client is not reading
		}

		// try to send event to blocking client with timeout, if still not reading, evict this client
		go func(clientChan chan<- types.WatchEvent) {
			select {
			case clientChan <- event:
			case <-time.After(blockingClientEvictionTimeout):
				m.logger.Warn("Client not reading events, evicting",
					zap.String("key", key))
				m.removeClient(key, clientChan)
				close(clientChan)
			case <-ctx.Done():
			}
		}(client)
	}
}
