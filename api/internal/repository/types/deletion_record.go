package types

import (
	"time"

	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type DeletionRecord struct {
	ObjectKey sdkmeta.ObjectKey `json:"objectKey"`
	Timestamp time.Time         `json:"timestamp"`
}

func NewDeletionRecord(objectKey sdkmeta.ObjectKey) *DeletionRecord {
	return &DeletionRecord{
		ObjectKey: objectKey,
		Timestamp: time.Now(),
	}
}

type DeletionBatch struct {
	ObjectKeys []sdkmeta.ObjectKey
	LeaseID    clientv3.LeaseID
}
