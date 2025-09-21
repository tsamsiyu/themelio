package repository

import (
	"context"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

// KeyValue represents a key-value pair from etcd
type KeyValue struct {
	Key   string
	Value []byte
}

// ClientWrapper provides a thin generic wrapper over etcd client
type ClientWrapper interface {
	// Basic CRUD operations with raw data
	Put(ctx context.Context, key string, value string) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string, limit int) ([]KeyValue, error)

	// Transaction execution
	ExecuteTransaction(ctx context.Context, ops []clientv3.Op) error
	ExecuteConditionalTransaction(ctx context.Context, compare []clientv3.Cmp, successOps []clientv3.Op, failureOps []clientv3.Op) (*clientv3.TxnResponse, error)

	// Lease management
	GrantLease(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	RevokeLease(ctx context.Context, leaseID clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	KeepAliveLease(ctx context.Context, leaseID clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)

	// Watch operations
	Watch(ctx context.Context, prefix string) (<-chan clientv3.WatchResponse, error)
}

type clientWrapper struct {
	logger *zap.Logger
	client *clientv3.Client
}

func NewClientWrapper(logger *zap.Logger, client *clientv3.Client) ClientWrapper {
	return &clientWrapper{
		logger: logger,
		client: client,
	}
}

// ClientWrapper implementation
func (c *clientWrapper) Put(ctx context.Context, key string, value string) error {
	_, err := c.client.Put(ctx, key, value)
	return err
}

func (c *clientWrapper) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, types.NewNotFoundError(key)
	}

	return resp.Kvs[0].Value, nil
}

func (c *clientWrapper) Delete(ctx context.Context, key string) error {
	_, err := c.client.Delete(ctx, key)
	return err
}

func (c *clientWrapper) List(ctx context.Context, prefix string, limit int) ([]KeyValue, error) {
	opts := []clientv3.OpOption{clientv3.WithPrefix()}

	if limit > 0 {
		opts = append(opts, clientv3.WithLimit(int64(limit)))
	}

	resp, err := c.client.Get(ctx, prefix, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list from etcd")
	}

	var kvs []KeyValue
	for _, kv := range resp.Kvs {
		kvs = append(kvs, KeyValue{
			Key:   string(kv.Key),
			Value: kv.Value,
		})
	}

	return kvs, nil
}

func (c *clientWrapper) ExecuteTransaction(ctx context.Context, ops []clientv3.Op) error {
	txn := c.client.Txn(ctx)
	txnResp, err := txn.Then(ops...).Commit()
	if err != nil {
		return errors.Wrap(err, "failed to execute transaction")
	}

	if !txnResp.Succeeded {
		return errors.New("transaction failed")
	}

	return nil
}

func (c *clientWrapper) ExecuteConditionalTransaction(ctx context.Context, compare []clientv3.Cmp, successOps []clientv3.Op, failureOps []clientv3.Op) (*clientv3.TxnResponse, error) {
	txn := c.client.Txn(ctx)

	if len(compare) > 0 {
		txn = txn.If(compare...)
	}

	if len(successOps) > 0 {
		txn = txn.Then(successOps...)
	}

	if len(failureOps) > 0 {
		txn = txn.Else(failureOps...)
	}

	txnResp, err := txn.Commit()
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute conditional transaction")
	}

	return txnResp, nil
}

func (c *clientWrapper) GrantLease(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	lease, err := c.client.Grant(ctx, ttl)
	if err != nil {
		return nil, errors.Wrap(err, "failed to grant lease")
	}
	return lease, nil
}

func (c *clientWrapper) RevokeLease(ctx context.Context, leaseID clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	resp, err := c.client.Revoke(ctx, leaseID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to revoke lease")
	}
	return resp, nil
}

func (c *clientWrapper) KeepAliveLease(ctx context.Context, leaseID clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch, err := c.client.KeepAlive(ctx, leaseID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to keep alive lease")
	}
	return ch, nil
}

func (c *clientWrapper) Watch(ctx context.Context, prefix string) (<-chan clientv3.WatchResponse, error) {
	watchChan := c.client.Watch(ctx, prefix, clientv3.WithPrefix())
	return watchChan, nil
}
