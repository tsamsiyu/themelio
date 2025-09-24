package types

import (
	"time"

	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type WatchEventType string

const (
	WatchEventTypeAdded    WatchEventType = "added"
	WatchEventTypeModified WatchEventType = "modified"
	WatchEventTypeDeleted  WatchEventType = "deleted"
	WatchEventTypeError    WatchEventType = "error"
)

type WatchEvent struct {
	Type      WatchEventType  `json:"type"`
	Object    *sdkmeta.Object `json:"object"`
	Timestamp time.Time       `json:"timestamp"`
	Revision  int64           `json:"revision,omitempty"`
	Error     error           `json:"error,omitempty"`
}
