package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/api/middleware"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	"github.com/tsamsiyu/themelio/api/mocks"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

func TestWatchHandler_WatchResource(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logger := zap.NewNop()

	mockService := mocks.NewMockResourceService(t)
	handler := NewWatchHandler(logger, mockService)

	eventChan := make(chan types.WatchEvent, 1)
	mockService.EXPECT().WatchResource(mock.Anything, mock.Anything, int64(0)).Return((<-chan types.WatchEvent)(eventChan), nil)

	router := gin.New()
	router.GET("/test/:group/:version/:kind/watch", handler.WatchResource)

	req, _ := http.NewRequest("GET", "/test/testgroup/v1/testkind/watch", nil)
	w := httptest.NewRecorder()

	go func() {
		time.Sleep(10 * time.Millisecond)
		eventChan <- types.WatchEvent{
			Type:      types.WatchEventTypeAdded,
			Object:    &sdkmeta.Object{},
			Timestamp: time.Now(),
			Revision:  1,
		}
		close(eventChan)
	}()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
	assert.Equal(t, "keep-alive", w.Header().Get("Connection"))

	body := w.Body.String()
	assert.Contains(t, body, "event: connected")
	assert.Contains(t, body, "event: event")
	assert.Contains(t, body, "\"type\":\"added\"")
}

func TestWatchHandler_WatchResource_WithRevision(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logger := zap.NewNop()

	mockService := mocks.NewMockResourceService(t)
	handler := NewWatchHandler(logger, mockService)

	eventChan := make(chan types.WatchEvent, 1)
	mockService.EXPECT().WatchResource(mock.Anything, mock.Anything, int64(123)).Return((<-chan types.WatchEvent)(eventChan), nil)

	router := gin.New()
	router.GET("/test/:group/:version/:kind/watch", handler.WatchResource)

	req, _ := http.NewRequest("GET", "/test/testgroup/v1/testkind/watch?revision=123", nil)
	w := httptest.NewRecorder()

	go func() {
		time.Sleep(10 * time.Millisecond)
		close(eventChan)
	}()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestWatchHandler_WatchResource_InvalidRevision(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logger := zap.NewNop()

	mockService := mocks.NewMockResourceService(t)
	handler := NewWatchHandler(logger, mockService)

	router := gin.New()
	router.Use(middleware.ErrorMapper(logger))
	router.GET("/test/:group/:version/:kind/watch", handler.WatchResource)

	req, _ := http.NewRequest("GET", "/test/testgroup/v1/testkind/watch?revision=invalid", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestWatchHandler_sendSSEEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logger := zap.NewNop()

	mockService := mocks.NewMockResourceService(t)
	handler := NewWatchHandler(logger, mockService)

	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")

		data := map[string]interface{}{
			"message":   "test",
			"timestamp": time.Now().UTC(),
		}

		err := handler.sendSSEEvent(c, "test-event", data)
		assert.NoError(t, err)
	})

	req, _ := http.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "event: test-event")
	assert.Contains(t, body, "data:")

	var eventData map[string]interface{}
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "data: ") {
			jsonStr := strings.TrimPrefix(line, "data: ")
			err := json.Unmarshal([]byte(jsonStr), &eventData)
			assert.NoError(t, err)
			break
		}
	}

	assert.Equal(t, "test", eventData["message"])
}

func TestWatchHandler_sendSSEEventWithRetry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logger := zap.NewNop()

	mockService := mocks.NewMockResourceService(t)
	handler := NewWatchHandler(logger, mockService)

	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")

		data := map[string]interface{}{
			"message":   "test-retry",
			"timestamp": time.Now().UTC(),
		}

		err := handler.sendSSEEventWithRetry(c, "test-retry-event", data)
		assert.NoError(t, err)
	})

	req, _ := http.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "event: test-retry-event")
	assert.Contains(t, body, "data:")

	var eventData map[string]interface{}
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "data: ") {
			jsonStr := strings.TrimPrefix(line, "data: ")
			err := json.Unmarshal([]byte(jsonStr), &eventData)
			assert.NoError(t, err)
			break
		}
	}

	assert.Equal(t, "test-retry", eventData["message"])
}
