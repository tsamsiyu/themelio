package types

import (
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type WatchEventType string

const (
	WatchEventTypeAdded    WatchEventType = "added"
	WatchEventTypeModified WatchEventType = "modified"
	WatchEventTypeDeleted  WatchEventType = "deleted"
	WatchEventTypeError    WatchEventType = "error"
)

type WatchEvent struct {
	Type      WatchEventType             `json:"type"`
	Object    *unstructured.Unstructured `json:"object"`
	Timestamp time.Time                  `json:"timestamp"`
	Revision  int64                      `json:"revision,omitempty"`
	Error     error                      `json:"error,omitempty"`
}
