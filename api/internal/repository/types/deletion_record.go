package types

import (
	"time"
)

// DeletionRecord represents a deletion record stored in etcd
type DeletionRecord struct {
	ObjectKey ObjectKey `json:"objectKey"`
	Timestamp time.Time `json:"timestamp"`
}

// NewDeletionRecord creates a new deletion record
func NewDeletionRecord(objectKey ObjectKey) *DeletionRecord {
	return &DeletionRecord{
		ObjectKey: objectKey,
		Timestamp: time.Now(),
	}
}
