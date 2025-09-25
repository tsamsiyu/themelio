package repository

import (
	"context"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type clientWrapper struct {
	logger *zap.Logger
	client *clientv3.Client
}

func NewClientWrapper(logger *zap.Logger, client *clientv3.Client) types.ClientWrapper {
	return &clientWrapper{
		logger: logger,
		client: client,
	}
}

func (c *clientWrapper) Client() types.EtcdClientInterface {
	return &etcdClientAdapter{client: c.client}
}

// etcdClientAdapter adapts *clientv3.Client to EtcdClientInterface
type etcdClientAdapter struct {
	client *clientv3.Client
}

func (a *etcdClientAdapter) Txn(ctx context.Context) clientv3.Txn {
	return a.client.Txn(ctx)
}

func (c *clientWrapper) Put(ctx context.Context, key string, value string) error {
	_, err := c.client.Put(ctx, key, value)
	return err
}

func (c *clientWrapper) Get(ctx context.Context, key string) (*types.KeyValue, error) {
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, NewNotFoundError(key)
	}

	kv := convertClientKV(resp.Kvs[0])

	return &kv, nil
}

func (c *clientWrapper) Delete(ctx context.Context, key string) error {
	_, err := c.client.Delete(ctx, key)
	return err
}

func (c *clientWrapper) List(ctx context.Context, paging types.Paging) (*types.Batch, error) {
	opts := make([]clientv3.OpOption, 0, 5)

	queryKey := paging.Prefix

	if paging.Limit > 0 {
		if paging.LastKey != "" {
			opts = append(opts, clientv3.WithLastKey()...)
			opts = append(opts, clientv3.WithLimit(int64(paging.Limit+1)))
			queryKey = paging.LastKey
		} else {
			opts = append(opts, clientv3.WithPrefix())
			opts = append(opts, clientv3.WithLimit(int64(paging.Limit)))
		}
	}

	if paging.MinModRevision > 0 {
		opts = append(opts, clientv3.WithMinModRev(paging.MinModRevision))
	}

	if paging.SortDesc {
		opts = append(opts, clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	} else {
		opts = append(opts, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	}

	if queryKey == "" {
		return nil, errors.New("prefix or last key is required")
	}

	resp, err := c.client.Get(ctx, queryKey, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list from etcd with prefix %s", paging.Prefix)
	}

	var kvs []types.KeyValue
	for i, kv := range resp.Kvs {
		if i == 0 && !paging.IncludeLastKeyInBatch {
			continue
		}
		kvs = append(kvs, convertClientKV(kv))
	}

	return &types.Batch{
		Revision: resp.Header.Revision,
		KVs:      kvs,
	}, nil
}

func (c *clientWrapper) ExecuteTransaction(ctx context.Context, ops []clientv3.Op) (*clientv3.TxnResponse, error) {
	txn := c.client.Txn(ctx)
	txnResp, err := txn.Then(ops...).Commit()
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute transaction")
	}

	if !txnResp.Succeeded {
		return nil, errors.New("transaction failed")
	}

	return txnResp, nil
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

func (c *clientWrapper) Watch(ctx context.Context, prefix string, revision ...int64) (<-chan clientv3.WatchResponse, error) {
	opts := []clientv3.OpOption{clientv3.WithPrefix()}

	if len(revision) > 0 && revision[0] > 0 {
		opts = append(opts, clientv3.WithRev(revision[0]))
	}

	watchChan := c.client.Watch(ctx, prefix, opts...)
	return watchChan, nil
}

func convertClientKV(kv *mvccpb.KeyValue) types.KeyValue {
	return types.KeyValue{
		Key:            string(kv.Key),
		Version:        kv.Version,
		ModRevision:    kv.ModRevision,
		CreateRevision: kv.CreateRevision,
		Value:          kv.Value,
	}
}
