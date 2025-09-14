package service

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsamsiyu/themelio/api/internal/repository"
)

// mockResourceRepository is a mock implementation of ResourceRepository for testing
type mockResourceRepository struct {
	watchEvents chan repository.WatchEvent
	listResult  []*unstructured.Unstructured
	listError   error
}

func newMockResourceRepository() *mockResourceRepository {
	return &mockResourceRepository{
		watchEvents: make(chan repository.WatchEvent, 10),
		listResult:  make([]*unstructured.Unstructured, 0),
		listError:   nil,
	}
}

func (m *mockResourceRepository) Replace(ctx context.Context, key repository.ObjectKey, resource *unstructured.Unstructured) error {
	return nil
}

func (m *mockResourceRepository) Get(ctx context.Context, key repository.ObjectKey) (*unstructured.Unstructured, error) {
	return nil, nil
}

func (m *mockResourceRepository) List(ctx context.Context, key repository.ObjectKey) ([]*unstructured.Unstructured, error) {
	return m.listResult, m.listError
}

func (m *mockResourceRepository) Delete(ctx context.Context, key repository.ObjectKey) error {
	return nil
}

func (m *mockResourceRepository) Watch(ctx context.Context, key repository.ObjectKey, eventChan chan<- repository.WatchEvent) error {
	go func() {
		for event := range m.watchEvents {
			select {
			case eventChan <- event:
			case <-ctx.Done():
				return
			}
		}
		close(eventChan)
	}()
	return nil
}

func (m *mockResourceRepository) sendEvent(event repository.WatchEvent) {
	m.watchEvents <- event
}

func (m *mockResourceRepository) setListResult(resources []*unstructured.Unstructured) {
	m.listResult = resources
}

func (m *mockResourceRepository) setListError(err error) {
	m.listError = err
}

func TestResourceWatchService_Watch(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := newMockResourceRepository()
	config := &WatchConfig{
		PollInterval:      100 * time.Millisecond,
		CacheTimeout:      1 * time.Second,
		DisableBackground: true,
	}

	service := NewResourceWatchService(logger, mockRepo, config)
	defer close(service.stopChan)

	objectKey := repository.ObjectKey{
		Group:     "test",
		Version:   "v1",
		Kind:      "TestResource",
		Namespace: "default",
		Name:      "",
	}

	// Test basic watch functionality
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	eventChan := service.Watch(ctx, objectKey)

	// Send a test event
	testResource := &unstructured.Unstructured{}
	testResource.SetAPIVersion("test/v1")
	testResource.SetKind("TestResource")
	testResource.SetName("test-resource")
	testResource.SetNamespace(objectKey.Namespace)
	testResource.SetResourceVersion("1")

	testEvent := repository.WatchEvent{
		Type:      repository.WatchEventTypeAdded,
		Object:    testResource,
		Timestamp: time.Now(),
	}

	mockRepo.sendEvent(testEvent)

	// Wait for event
	select {
	case event := <-eventChan:
		if event.Type != repository.WatchEventTypeAdded {
			t.Errorf("Expected event type %s, got %s", repository.WatchEventTypeAdded, event.Type)
		}
		if event.Object.GetName() != "test-resource" {
			t.Errorf("Expected resource name 'test-resource', got '%s'", event.Object.GetName())
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("Timeout waiting for event")
	}
}

func TestResourceWatchService_MultipleClients(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := newMockResourceRepository()
	config := &WatchConfig{
		PollInterval:      100 * time.Millisecond,
		CacheTimeout:      1 * time.Second,
		DisableBackground: true,
	}

	service := NewResourceWatchService(logger, mockRepo, config)
	defer close(service.stopChan)

	objectKey := repository.ObjectKey{
		Group:     "test",
		Version:   "v1",
		Kind:      "TestResource",
		Namespace: "default",
		Name:      "",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Create multiple clients watching the same resources
	client1Chan := service.Watch(ctx, objectKey)
	client2Chan := service.Watch(ctx, objectKey)

	// Send a test event
	testResource := &unstructured.Unstructured{}
	testResource.SetAPIVersion("test/v1")
	testResource.SetKind("TestResource")
	testResource.SetName("test-resource")
	testResource.SetNamespace(objectKey.Namespace)
	testResource.SetResourceVersion("1")

	testEvent := repository.WatchEvent{
		Type:      repository.WatchEventTypeAdded,
		Object:    testResource,
		Timestamp: time.Now(),
	}

	mockRepo.sendEvent(testEvent)

	// Both clients should receive the event
	client1Received := false
	client2Received := false

	// Wait for both clients to receive the event
	timeout := time.After(2 * time.Second)
	for !client1Received || !client2Received {
		select {
		case event := <-client1Chan:
			t.Logf("Client 1 received event: %s", event.Type)
			if event.Type == repository.WatchEventTypeAdded {
				client1Received = true
			}
		case event := <-client2Chan:
			t.Logf("Client 2 received event: %s", event.Type)
			if event.Type == repository.WatchEventTypeAdded {
				client2Received = true
			}
		case <-timeout:
			t.Errorf("Timeout waiting for events. Client1: %v, Client2: %v", client1Received, client2Received)
			return
		}
	}

	if !client1Received {
		t.Error("Client 1 did not receive the event")
	}
	if !client2Received {
		t.Error("Client 2 did not receive the event")
	}
}

func TestResourceWatchService_EventReconciliation(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := newMockResourceRepository()
	config := &WatchConfig{
		PollInterval:      50 * time.Millisecond,
		CacheTimeout:      1 * time.Second,
		DisableBackground: false,
	}

	service := NewResourceWatchService(logger, mockRepo, config)
	defer close(service.stopChan)

	objectKey := repository.ObjectKey{
		Group:     "test",
		Version:   "v1",
		Kind:      "TestResource",
		Namespace: "default",
		Name:      "",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	eventChan := service.Watch(ctx, objectKey)

	// First, send an initial event to populate the cache
	initialResource := &unstructured.Unstructured{}
	initialResource.SetAPIVersion("test/v1")
	initialResource.SetKind("TestResource")
	initialResource.SetName("initial-resource")
	initialResource.SetNamespace(objectKey.Namespace)
	initialResource.SetResourceVersion("1")

	initialEvent := repository.WatchEvent{
		Type:      repository.WatchEventTypeAdded,
		Object:    initialResource,
		Timestamp: time.Now(),
	}

	mockRepo.sendEvent(initialEvent)

	// Wait for the initial event to be processed
	select {
	case event := <-eventChan:
		if event.Type != repository.WatchEventTypeAdded {
			t.Errorf("Expected initial event type %s, got %s", repository.WatchEventTypeAdded, event.Type)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for initial event")
		return
	}

	// Now set up a different resource in the list result for reconciliation
	testResource := &unstructured.Unstructured{}
	testResource.SetAPIVersion("test/v1")
	testResource.SetKind("TestResource")
	testResource.SetName("reconciled-resource")
	testResource.SetNamespace(objectKey.Namespace)
	testResource.SetResourceVersion("2")

	mockRepo.setListResult([]*unstructured.Unstructured{testResource})

	// Wait for reconciliation events (deleted for initial-resource, added for reconciled-resource)
	timeout := time.After(2 * time.Second)
	deletedReceived := false
	addedReceived := false

	for !deletedReceived || !addedReceived {
		select {
		case event := <-eventChan:
			if event.Type == repository.WatchEventTypeDeleted && event.Object.GetName() == "initial-resource" {
				deletedReceived = true
			} else if event.Type == repository.WatchEventTypeAdded && event.Object.GetName() == "reconciled-resource" {
				addedReceived = true
			}
		case <-timeout:
			t.Errorf("Timeout waiting for reconciliation events. Deleted: %v, Added: %v", deletedReceived, addedReceived)
			return
		}
	}
}
