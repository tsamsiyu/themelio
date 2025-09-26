package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	"github.com/tsamsiyu/themelio/api/mocks"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

func TestWatchHandler_processWatchEvents_Added(t *testing.T) {
	handler, eventChan := createTestWatchHandler(t)
	watchChan := make(chan types.WatchEvent, 1)

	obj := createTestObject(t)
	event := types.WatchEvent{
		Type:      types.WatchEventTypeAdded,
		Object:    obj,
		Timestamp: time.Now(),
		Revision:  123,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchChan <- event
	close(watchChan)

	err := handler.ProcessWatchEvents(ctx, watchChan)

	assert.NoError(t, err)
	assert.Equal(t, int64(123), handler.LastRevision)

	key := *obj.ObjectKey
	cachedEntry, exists := handler.Cache[key]
	assert.True(t, exists)
	assert.Equal(t, obj.SystemMeta.Version, cachedEntry.Version)
	assert.Equal(t, obj.SystemMeta.ModRevision, cachedEntry.ModRevision)
	assert.Equal(t, obj.SystemMeta.CreateRevision, cachedEntry.CreateRevision)

	// Verify that the event was sent to the handler's internal channel
	select {
	case receivedEvent := <-eventChan:
		assert.Equal(t, types.WatchEventTypeAdded, receivedEvent.Type)
		assert.Equal(t, obj, receivedEvent.Object)
	case <-time.After(time.Second):
		t.Fatal("Handler did not send event to internal channel")
	}
}

func TestWatchHandler_processWatchEvents_Modified(t *testing.T) {
	handler, eventChan := createTestWatchHandler(t)
	watchChan := make(chan types.WatchEvent, 1)

	obj := createTestObject(t)
	event := types.WatchEvent{
		Type:      types.WatchEventTypeModified,
		Object:    obj,
		Timestamp: time.Now(),
		Revision:  124,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchChan <- event
	close(watchChan)

	err := handler.ProcessWatchEvents(ctx, watchChan)

	assert.NoError(t, err)
	assert.Equal(t, int64(124), handler.LastRevision)

	key := *obj.ObjectKey
	cachedEntry, exists := handler.Cache[key]
	assert.True(t, exists)
	assert.Equal(t, obj.SystemMeta.Version, cachedEntry.Version)

	select {
	case receivedEvent := <-eventChan:
		assert.Equal(t, types.WatchEventTypeModified, receivedEvent.Type)
	case <-time.After(time.Second):
		t.Fatal("Handler did not send event to internal channel")
	}
}

func TestWatchHandler_processWatchEvents_Deleted(t *testing.T) {
	handler, eventChan := createTestWatchHandler(t)
	watchChan := make(chan types.WatchEvent, 1)

	obj := createTestObject(t)
	key := *obj.ObjectKey

	handler.Cache[key] = types.WatchCacheEntry{
		Version:        1,
		CreateRevision: 100,
		ModRevision:    101,
	}

	event := types.WatchEvent{
		Type:      types.WatchEventTypeDeleted,
		Object:    obj,
		Timestamp: time.Now(),
		Revision:  125,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchChan <- event
	close(watchChan)

	err := handler.ProcessWatchEvents(ctx, watchChan)

	assert.NoError(t, err)
	assert.Equal(t, int64(125), handler.LastRevision)

	_, exists := handler.Cache[key]
	assert.False(t, exists)

	select {
	case receivedEvent := <-eventChan:
		assert.Equal(t, types.WatchEventTypeDeleted, receivedEvent.Type)
	case <-time.After(time.Second):
		t.Fatal("Handler did not send event to internal channel")
	}
}

func TestWatchHandler_processWatchEvents_Error(t *testing.T) {
	handler, _ := createTestWatchHandler(t)
	watchChan := make(chan types.WatchEvent, 1)

	testError := errors.New("test error")
	event := types.WatchEvent{
		Type:      types.WatchEventTypeError,
		Error:     testError,
		Timestamp: time.Now(),
		Revision:  126,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchChan <- event
	close(watchChan)

	err := handler.ProcessWatchEvents(ctx, watchChan)

	assert.Error(t, err)
	assert.Equal(t, testError, err)
}

func TestWatchHandler_processWatchEvents_ContextCanceled(t *testing.T) {
	handler, _ := createTestWatchHandler(t)
	watchChan := make(chan types.WatchEvent, 1)

	event := types.WatchEvent{
		Type:      types.WatchEventTypeError,
		Error:     context.Canceled,
		Timestamp: time.Now(),
		Revision:  127,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchChan <- event
	close(watchChan)

	err := handler.ProcessWatchEvents(ctx, watchChan)

	assert.NoError(t, err)
}

func TestWatchHandler_processWatchEvents_ChannelClosed(t *testing.T) {
	handler, _ := createTestWatchHandler(t)
	watchChan := make(chan types.WatchEvent, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	close(watchChan)

	err := handler.ProcessWatchEvents(ctx, watchChan)

	assert.NoError(t, err)
}

func TestWatchHandler_reconcile_NewResource(t *testing.T) {
	handler, eventChan := createTestWatchHandler(t)

	obj := createTestObject(t)
	batch := &types.ObjectBatch{
		Revision: 200,
		Objects:  []*sdkmeta.Object{obj},
	}

	mockStore := handler.Store.(*mocks.MockResourceStore)
	mockStore.EXPECT().List(mock.Anything, handler.ObjType, mock.AnythingOfType("*types.Paging")).
		Return(batch, nil)

	ctx := context.Background()
	err := handler.Reconcile(ctx)

	assert.NoError(t, err)
	assert.Equal(t, int64(200), handler.LastRevision)

	key := *obj.ObjectKey
	cachedEntry, exists := handler.Cache[key]
	assert.True(t, exists)
	assert.Equal(t, obj.SystemMeta.Version, cachedEntry.Version)

	select {
	case receivedEvent := <-eventChan:
		assert.Equal(t, types.WatchEventTypeAdded, receivedEvent.Type)
		assert.Equal(t, obj, receivedEvent.Object)
	case <-time.After(time.Second):
		t.Fatal("Handler did not send event to internal channel")
	}
}

func TestWatchHandler_reconcile_ModifiedResource(t *testing.T) {
	handler, eventChan := createTestWatchHandler(t)

	obj := createTestObject(t)
	obj.SystemMeta.ModRevision = 102 // Different from cache
	key := *obj.ObjectKey

	handler.Cache[key] = types.WatchCacheEntry{
		Version:        1,
		CreateRevision: 100,
		ModRevision:    101,
	}

	batch := &types.ObjectBatch{
		Revision: 200,
		Objects:  []*sdkmeta.Object{obj},
	}

	mockStore := handler.Store.(*mocks.MockResourceStore)
	mockStore.EXPECT().List(mock.Anything, handler.ObjType, mock.AnythingOfType("*types.Paging")).
		Return(batch, nil)

	ctx := context.Background()
	err := handler.Reconcile(ctx)

	assert.NoError(t, err)

	select {
	case receivedEvent := <-eventChan:
		assert.Equal(t, types.WatchEventTypeModified, receivedEvent.Type)
		assert.Equal(t, obj, receivedEvent.Object)
	case <-time.After(time.Second):
		t.Fatal("Handler did not send event to internal channel")
	}
}

func TestWatchHandler_reconcile_DeletedResource(t *testing.T) {
	handler, eventChan := createTestWatchHandler(t)

	obj := createTestObject(t)
	key := *obj.ObjectKey

	handler.Cache[key] = types.WatchCacheEntry{
		Version:        1,
		CreateRevision: 100,
		ModRevision:    101,
	}

	batch := &types.ObjectBatch{
		Revision: 200,
		Objects:  []*sdkmeta.Object{},
	}

	mockStore := handler.Store.(*mocks.MockResourceStore)
	mockStore.EXPECT().List(mock.Anything, handler.ObjType, mock.AnythingOfType("*types.Paging")).
		Return(batch, nil)

	ctx := context.Background()
	err := handler.Reconcile(ctx)

	assert.NoError(t, err)

	_, exists := handler.Cache[key]
	assert.False(t, exists)

	select {
	case receivedEvent := <-eventChan:
		assert.Equal(t, types.WatchEventTypeDeleted, receivedEvent.Type)
		assert.Equal(t, key, receivedEvent.ObjectKey)
	case <-time.After(time.Second):
		t.Fatal("Handler did not send event to internal channel")
	}
}

func TestWatchHandler_reconcile_RecreatedResource(t *testing.T) {
	handler, eventChan := createTestWatchHandler(t)

	obj := createTestObject(t)
	key := *obj.ObjectKey

	handler.Cache[key] = types.WatchCacheEntry{
		Version:        1,
		CreateRevision: 100,
		ModRevision:    101,
	}

	handler.LastRevision = 150

	obj.SystemMeta.CreateRevision = 200
	batch := &types.ObjectBatch{
		Revision: 200,
		Objects:  []*sdkmeta.Object{obj},
	}

	mockStore := handler.Store.(*mocks.MockResourceStore)
	mockStore.EXPECT().List(mock.Anything, handler.ObjType, mock.AnythingOfType("*types.Paging")).
		Return(batch, nil)

	ctx := context.Background()
	err := handler.Reconcile(ctx)
	assert.NoError(t, err)

	events := make([]types.WatchEvent, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case receivedEvent := <-eventChan:
			events = append(events, receivedEvent)
		case <-time.After(time.Second):
			t.Fatal("Client did not receive expected events")
		}
	}

	assert.Equal(t, types.WatchEventTypeDeleted, events[0].Type)
	assert.Equal(t, key, events[0].ObjectKey)
	assert.Equal(t, types.WatchEventTypeAdded, events[1].Type)
	assert.Equal(t, obj, events[1].Object)
}

func TestWatchHandler_reconcile_ListError(t *testing.T) {
	handler, _ := createTestWatchHandler(t)

	testError := errors.New("list error")
	mockStore := handler.Store.(*mocks.MockResourceStore)
	mockStore.EXPECT().List(mock.Anything, handler.ObjType, mock.AnythingOfType("*types.Paging")).
		Return(nil, testError)

	ctx := context.Background()
	err := handler.Reconcile(ctx)

	assert.Error(t, err)
	assert.Equal(t, testError, err)
}

func TestWatchHandler_reconcile_BatchProcessing(t *testing.T) {
	handler, eventChan := createTestWatchHandler(t)
	handler.config.ReconcileBatchSize = 1

	obj1 := createTestObject(t)
	obj1.ObjectKey.Name = "resource-1"
	obj2 := createTestObject(t)
	obj2.ObjectKey.Name = "resource-2"

	batch1 := &types.ObjectBatch{
		Revision: 200,
		Objects:  []*sdkmeta.Object{obj1},
	}

	batch2 := &types.ObjectBatch{
		Revision: 200,
		Objects:  []*sdkmeta.Object{},
	}

	mockStore := handler.Store.(*mocks.MockResourceStore)
	mockStore.EXPECT().List(mock.Anything, handler.ObjType, mock.AnythingOfType("*types.Paging")).
		Return(batch1, nil).Once()
	mockStore.EXPECT().List(mock.Anything, handler.ObjType, mock.AnythingOfType("*types.Paging")).
		Return(batch2, nil).Once()

	ctx := context.Background()
	err := handler.Reconcile(ctx)

	assert.NoError(t, err)

	select {
	case receivedEvent := <-eventChan:
		assert.Equal(t, types.WatchEventTypeAdded, receivedEvent.Type)
		assert.Equal(t, obj1, receivedEvent.Object)
	case <-time.After(time.Second):
		t.Fatal("Handler did not send event to internal channel")
	}
}

func createTestWatchHandler(t *testing.T) (*WatchHandler, <-chan types.WatchEvent) {
	objType := &sdkmeta.ObjectType{
		Group:     "example.com",
		Version:   "v1",
		Kind:      "TestResource",
		Namespace: "default",
	}
	store := mocks.NewMockResourceStore(t)
	logger := zap.NewNop()
	config := types.WatchConfig{
		MaxRetries:         3,
		ReconcileBatchSize: 100,
	}
	backoff := lib.NewBackoffManager(lib.BackoffConfig{
		InitialBackoff:    time.Second,
		MaxBackoff:        time.Minute,
		BackoffMultiplier: 2.0,
		ResetAfter:        time.Hour,
	})

	handler := NewWatchHandler(objType, store, logger, config, backoff, 0)
	return handler, handler.EventChannel()
}

func createTestObject(t *testing.T) *sdkmeta.Object {
	now := time.Now()
	return &sdkmeta.Object{
		ObjectKey: &sdkmeta.ObjectKey{
			ObjectType: sdkmeta.ObjectType{
				Group:     "example.com",
				Version:   "v1",
				Kind:      "TestResource",
				Namespace: "default",
			},
			Name: "test-resource",
		},
		ObjectMeta: &sdkmeta.ObjectMeta{
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		SystemMeta: &sdkmeta.SystemMeta{
			UID:            "test-uid",
			Version:        1,
			CreateRevision: 100,
			ModRevision:    101,
			CreationTime:   &now,
		},
		Spec: map[string]interface{}{},
	}
}
