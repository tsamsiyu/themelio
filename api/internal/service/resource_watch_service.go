package service

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsamsiyu/themelio/api/internal/repository"
)

const WATCH_BUFFER_SIZE = 100
const MAX_QUERY_TIMEOUT = 3 * time.Second

// ResourceCacheEntry represents a cached resource entry
type ResourceCacheEntry struct {
	ResourceVersion string
	LastSeen        time.Time
}

// WatchConfig contains configuration for the ResourceWatchService
type WatchConfig struct {
	// PollInterval defines how often to poll for missing events
	PollInterval time.Duration

	// CacheTimeout defines when to remove stale cache entries
	CacheTimeout time.Duration

	// DisableBackground disables the background reconciliation process (for testing)
	DisableBackground bool
}

// DefaultWatchConfig returns a default configuration
func DefaultWatchConfig() *WatchConfig {
	return &WatchConfig{
		PollInterval: 30 * time.Second,
		CacheTimeout: 5 * time.Minute,
	}
}

// ResourceWatchService provides a stateful wrapper over the resource repository watch method
type ResourceWatchService struct {
	logger *zap.Logger
	repo   repository.ResourceRepository

	// each entry is the cache entry for the given object key
	cache sync.Map // key: repository.ObjectKey, value: *ResourceCacheEntry

	// each entry is the list of clients listening to the same watch key
	clients sync.Map // key: repository.ObjectKey, value: []chan<- repository.WatchEvent

	// each entry is the repository channel for the given watch key
	subscriptions map[string]chan repository.WatchEvent // key: repository.ObjectKey, value: repository channel

	stopChan chan struct{}

	config *WatchConfig
}

// NewResourceWatchService creates a new ResourceWatchService
func NewResourceWatchService(logger *zap.Logger, repo repository.ResourceRepository, config *WatchConfig) *ResourceWatchService {
	if config == nil {
		config = DefaultWatchConfig()
	}

	service := &ResourceWatchService{
		logger:        logger,
		repo:          repo,
		subscriptions: make(map[string]chan repository.WatchEvent),
		stopChan:      make(chan struct{}),
		config:        config,
	}

	if !config.DisableBackground {
		go service.backgroundProcess()
	}

	return service
}

func (s *ResourceWatchService) Watch(ctx context.Context, objectKey repository.ObjectKey) <-chan repository.WatchEvent {
	watchKey := objectKey.ToKey()

	clientChan := make(chan repository.WatchEvent, WATCH_BUFFER_SIZE)

	existingClients, _ := s.clients.LoadOrStore(watchKey, []chan<- repository.WatchEvent{})
	clients := existingClients.([]chan<- repository.WatchEvent)

	clients = append(clients, clientChan)
	s.clients.Store(watchKey, clients)

	if len(clients) == 1 { // first client for this key
		s.subscribe(ctx, watchKey, objectKey)
	}

	return clientChan
}

func (s *ResourceWatchService) subscribe(ctx context.Context, watchKey string, objectKey repository.ObjectKey) {
	subscriptionChan := make(chan repository.WatchEvent)

	s.subscriptions[watchKey] = subscriptionChan

	s.repo.Watch(ctx, objectKey, subscriptionChan)

	go s.forwardEvents(watchKey, subscriptionChan)
}

func (s *ResourceWatchService) forwardEvents(watchKey string, subscriptionChan <-chan repository.WatchEvent) {
	defer delete(s.subscriptions, watchKey)

	for event := range subscriptionChan {
		s.updateCacheFromEvent(event)

		if clients, exists := s.clients.Load(watchKey); exists {
			for _, clientChan := range clients.([]chan<- repository.WatchEvent) {
				select {
				case clientChan <- event:
				default:
					s.logger.Warn("Client channel full, dropping event",
						zap.String("watchKey", watchKey),
						zap.String("eventType", string(event.Type)))
				}
			}
		}
	}
}

// updateCacheFromEvent updates the cache based on a watch event
func (s *ResourceWatchService) updateCacheFromEvent(event repository.WatchEvent) {
	if event.Object == nil {
		return
	}

	objectKey := repository.NewObjectKeyFromResource(event.Object)
	now := time.Now()

	switch event.Type {
	case repository.WatchEventTypeAdded, repository.WatchEventTypeModified:
		s.cache.Store(objectKey, &ResourceCacheEntry{
			ResourceVersion: event.Object.GetResourceVersion(),
			LastSeen:        now,
		})
	case repository.WatchEventTypeDeleted:
		s.cache.Delete(objectKey)
	}
}

// backgroundProcess runs the periodic polling and reconciliation
func (s *ResourceWatchService) backgroundProcess() {
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.pollAndReconcile()
		}
	}
}

// pollAndReconcile polls current resources and generates missing events
func (s *ResourceWatchService) pollAndReconcile() {
	s.clients.Range(func(watchKeyInterface, _ interface{}) bool {
		watchKeyString := watchKeyInterface.(string)
		watchKey, err := repository.ParseKey(watchKeyString)
		if err != nil {
			s.logger.Error("Invalid watch key", zap.String("watchKey", watchKeyString), zap.Error(err))
			return true
		}

		ctx, cancel := context.WithTimeout(context.Background(), MAX_QUERY_TIMEOUT)
		defer cancel()

		latestState, err := s.repo.List(ctx, watchKey)

		if err != nil {
			s.logger.Error("Failed to list resources during reconciliation",
				zap.Object("objectKey", watchKey),
				zap.Error(err))
			return true
		}

		s.compareAndGenerateEvents(watchKeyString, latestState)
		s.updateCache(latestState)

		return true
	})
}

// compareAndGenerateEvents compares current resources with cache and generates missing events
func (s *ResourceWatchService) compareAndGenerateEvents(watchKey string, latestState []*unstructured.Unstructured) {
	latestStateMap := make(map[repository.ObjectKey]*unstructured.Unstructured)
	for _, resource := range latestState {
		objectKey := repository.NewObjectKeyFromResource(resource)
		latestStateMap[objectKey] = resource
	}

	s.cache.Range(func(key, value interface{}) bool {
		objectKey := key.(repository.ObjectKey)
		if _, exists := latestStateMap[objectKey]; !exists {
			deletedObject := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name":      objectKey.Name,
						"namespace": objectKey.Namespace,
					},
				},
			}

			event := repository.WatchEvent{
				Type:      repository.WatchEventTypeDeleted,
				Object:    deletedObject,
				Timestamp: time.Now(),
			}

			s.broadcastEvent(watchKey, event)
		}

		return true
	})

	for objectKey, resource := range latestStateMap {
		if value, exists := s.cache.Load(objectKey); exists {
			cacheEntry := value.(*ResourceCacheEntry)
			if resource.GetResourceVersion() == cacheEntry.ResourceVersion {
				continue
			}
			event := repository.WatchEvent{
				Type:      repository.WatchEventTypeModified,
				Object:    resource,
				Timestamp: time.Now(),
			}
			s.broadcastEvent(watchKey, event)
		} else {
			event := repository.WatchEvent{
				Type:      repository.WatchEventTypeAdded,
				Object:    resource,
				Timestamp: time.Now(),
			}
			s.broadcastEvent(watchKey, event)
		}
	}
}

// updateCache updates the cache with current resources
func (s *ResourceWatchService) updateCache(resources []*unstructured.Unstructured) {
	now := time.Now()

	keys := make(map[string]bool)
	s.cache.Range(func(key, value interface{}) bool {
		objectKey := key.(repository.ObjectKey)
		keys[objectKey.ToKey()] = true
		return true
	})

	for _, resource := range resources {
		objectKey := repository.NewObjectKeyFromResource(resource)
		s.cache.Store(objectKey, &ResourceCacheEntry{
			ResourceVersion: resource.GetResourceVersion(),
			LastSeen:        now,
		})
		delete(keys, objectKey.ToKey())
	}

	for key := range keys {
		s.cache.Delete(key)
	}
}

// broadcastEvent broadcasts an event to all clients watching the given watch key
func (s *ResourceWatchService) broadcastEvent(watchKey string, event repository.WatchEvent) {
	if clients, exists := s.clients.Load(watchKey); exists {
		for _, clientChan := range clients.([]chan<- repository.WatchEvent) {
			select {
			case clientChan <- event:
			default:
				s.logger.Warn("Client channel full, dropping reconciliation event",
					zap.String("watchKey", watchKey),
					zap.String("eventType", string(event.Type)))
			}
		}
	}
}
