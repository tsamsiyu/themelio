package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	servicetypes "github.com/tsamsiyu/themelio/api/internal/service/types"
)

const (
	maxRetries = 3
	retryDelay = 100 * time.Millisecond
)

type WatchHandler struct {
	logger          *zap.Logger
	resourceService servicetypes.ResourceService
}

func NewWatchHandler(
	logger *zap.Logger,
	resourceService servicetypes.ResourceService,
) *WatchHandler {
	return &WatchHandler{
		logger:          logger,
		resourceService: resourceService,
	}
}

func (h *WatchHandler) WatchResource(c *gin.Context) {
	params, err := getParamsFromContext(c)
	if err != nil {
		c.Error(err)
		return
	}

	revisionStr := c.Query("revision")
	var revision int64
	if revisionStr != "" {
		revision, err = strconv.ParseInt(revisionStr, 10, 64)
		if err != nil {
			c.Error(internalerrors.NewInvalidInputError("invalid revision parameter"))
			return
		}
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Headers", "Cache-Control")

	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()

	watchChan, err := h.resourceService.WatchResource(ctx, params, revision)
	if err != nil {
		h.logger.Error("Failed to start resource watch",
			zap.String("group", params.Group),
			zap.String("version", params.Version),
			zap.String("kind", params.Kind),
			zap.String("namespace", params.Namespace),
			zap.Error(err))

		h.sendSSEError(c, "Failed to start watch", err)
		return
	}

	h.logger.Info("Started SSE watch for resource",
		zap.String("group", params.Group),
		zap.String("version", params.Version),
		zap.String("kind", params.Kind),
		zap.String("namespace", params.Namespace),
		zap.Int64("revision", revision))

	connectedEvent := map[string]interface{}{
		"type": "connected",
		"payload": map[string]interface{}{
			"message":   "Watch connection established",
			"timestamp": time.Now().UTC(),
			"revision":  revision,
			"params": map[string]interface{}{
				"group":     params.Group,
				"version":   params.Version,
				"kind":      params.Kind,
				"namespace": params.Namespace,
			},
		},
	}
	if err := h.sendSSEEventWithRetry(c, "connected", connectedEvent); err != nil {
		h.logger.Error("Failed to send connection event", zap.Error(err))
		return
	}

	ticker := time.NewTicker(30 * time.Second) // Heartbeat every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("SSE watch connection closed by client",
				zap.String("group", params.Group),
				zap.String("version", params.Version),
				zap.String("kind", params.Kind))
			return

		case event, ok := <-watchChan:
			if !ok {
				// todo: send alert here, it's etcd failure
				h.logger.Info("Watch channel closed, ending SSE stream",
					zap.String("group", params.Group),
					zap.String("version", params.Version),
					zap.String("kind", params.Kind))
				return
			}

			if err := h.sendWatchEventWithRetry(c, event); err != nil {
				h.logger.Error("Failed to send SSE event",
					zap.String("eventType", string(event.Type)),
					zap.Error(err))
				return
			}

		case <-ticker.C:
			heartbeatEvent := map[string]interface{}{
				"type": "heartbeat",
				"payload": map[string]interface{}{
					"timestamp": time.Now().UTC(),
				},
			}
			if err := h.sendSSEEventWithRetry(c, "heartbeat", heartbeatEvent); err != nil {
				h.logger.Error("Failed to send heartbeat", zap.Error(err))
				return
			}
		}
	}
}

func (h *WatchHandler) sendWatchEventWithRetry(c *gin.Context, event types.WatchEvent) error {
	resourceEventData := map[string]interface{}{
		"type":      event.Type,
		"timestamp": event.Timestamp,
		"revision":  event.Revision,
	}

	if event.Object != nil {
		resourceEventData["object"] = event.Object
	} else {
		resourceEventData["objectKey"] = event.ObjectKey
	}

	if event.Error != nil {
		resourceEventData["error"] = event.Error.Error()
	}

	watchEvent := map[string]interface{}{
		"type":    "event",
		"payload": resourceEventData,
	}

	return h.sendSSEEventWithRetry(c, "event", watchEvent)
}

func (h *WatchHandler) sendSSEEventWithRetry(c *gin.Context, eventType string, data interface{}) error {
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(retryDelay)
		}

		err := h.sendSSEEvent(c, eventType, data)
		if err == nil {
			return nil
		}

		h.logger.Warn("SSE event send failed",
			zap.String("eventType", eventType),
			zap.Int("attempt", attempt+1),
			zap.Int("maxRetries", maxRetries),
			zap.Error(err))
	}

	return fmt.Errorf("failed to send SSE event: retry attempts exceeded")
}

func (h *WatchHandler) sendSSEEvent(c *gin.Context, eventType string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal SSE event data: %w", err)
	}

	_, err = fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", eventType, string(jsonData))
	if err != nil {
		return fmt.Errorf("failed to write SSE event: %w", err)
	}

	c.Writer.Flush()
	return nil
}

func (h *WatchHandler) sendSSEError(c *gin.Context, message string, err error) {
	errorData := map[string]interface{}{
		"error":     message,
		"details":   err.Error(),
		"timestamp": time.Now().UTC(),
	}

	h.sendSSEEvent(c, "error", errorData)
}
