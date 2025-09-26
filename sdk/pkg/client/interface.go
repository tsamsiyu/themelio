package client

import (
	"context"
	"time"

	"github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type Params struct {
	Group     string
	Version   string
	Kind      string
	Namespace string
	Name      string
}

type WatchEventType string

const (
	WatchEventTypeEvent     WatchEventType = "event"
	WatchEventTypeConnected WatchEventType = "connected"
	WatchEventTypeHeartbeat WatchEventType = "heartbeat"
)

type ResourceEventType string

const (
	ResourceEventTypeAdded    ResourceEventType = "added"
	ResourceEventTypeModified ResourceEventType = "modified"
	ResourceEventTypeDeleted  ResourceEventType = "deleted"
	ResourceEventTypeError    ResourceEventType = "error"
)

type ResourceEvent struct {
	Type      ResourceEventType `json:"type"`
	Object    *meta.Object      `json:"object,omitempty"`
	ObjectKey meta.ObjectKey    `json:"objectKey,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Revision  int64             `json:"revision"`
	Error     string            `json:"error,omitempty"`
}

type ConnectedEvent struct {
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
	Revision  int64     `json:"revision"`
	Params    Params    `json:"params"`
}

type HeartbeatEvent struct {
	Timestamp time.Time `json:"timestamp"`
}

type WatchEvent struct {
	Type    WatchEventType `json:"type"`
	Payload interface{}    `json:"payload"`
}

type Client interface {
	ReplaceResource(ctx context.Context, params Params, jsonData []byte) error
	GetResource(ctx context.Context, params Params) (*meta.Object, error)
	ListResources(ctx context.Context, params Params) ([]*meta.Object, error)
	DeleteResource(ctx context.Context, params Params) error
	PatchResource(ctx context.Context, params Params, patchData []byte) (*meta.Object, error)
	WatchResource(ctx context.Context, params Params, revision int64) (<-chan WatchEvent, error)
}
