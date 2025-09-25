package repository

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdClientInterface is a minimal interface for etcd client that we can mock
// It includes the methods we need from clientv3.Client
type EtcdClientInterface interface {
	// Txn creates a transaction
	Txn(ctx context.Context) clientv3.Txn
}
