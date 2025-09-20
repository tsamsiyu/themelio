package types

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
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

// DeletionBatch represents a batch of locked deletion records
type DeletionBatch struct {
	ObjectKeys []ObjectKey
	LeaseID    clientv3.LeaseID
}
