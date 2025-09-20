package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type DeletionOpBuilder struct {
	store ResourceStore
}

func NewDeletionOpBuilder(store ResourceStore) *DeletionOpBuilder {
	return &DeletionOpBuilder{
		store: store,
	}
}

func (b *DeletionOpBuilder) BuildMarkDeletionOperation(
	ctx context.Context,
	key types.ObjectKey,
) (*clientv3.Op, error) {
	deletionKey := fmt.Sprintf("/deletion%s", key.String())
	deletionRecord := types.NewDeletionRecord(key)
	deletionData, err := json.Marshal(deletionRecord)
	if err != nil {
		return nil, internalerrors.NewMarshalingError("failed to marshal deletion record")
	}

	op := clientv3.OpPut(deletionKey, string(deletionData))

	return &op, nil
}

func (b *DeletionOpBuilder) BuildDeletionOperations(
	ctx context.Context,
	key types.ObjectKey,
	ownerRefs []metav1.OwnerReference,
	resource *unstructured.Unstructured,
	markDeletionObjectKeys []types.ObjectKey,
	removeReferencesObjectKeys []types.ObjectKey,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	if len(ownerRefs) == 0 {
		return ops, nil
	}

	for _, childKey := range markDeletionObjectKeys {
		deletionKey := fmt.Sprintf("/deletion%s", childKey.String())
		deletionRecord := types.NewDeletionRecord(childKey)
		deletionData, err := json.Marshal(deletionRecord)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal deletion record")
		}
		ops = append(ops, clientv3.OpPut(deletionKey, string(deletionData)))
	}

	for _, childKey := range removeReferencesObjectKeys {
		child, err := b.store.Get(ctx, childKey)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get child for owner reference removal")
		}

		if child == nil {
			continue
		}

		ownerRefs := child.GetOwnerReferences()
		var newOwnerRefs []metav1.OwnerReference
		for _, ref := range ownerRefs {
			refKey := types.OwnerRefToObjectKey(ref, childKey.Namespace)
			if refKey.String() != key.String() {
				newOwnerRefs = append(newOwnerRefs, ref)
			}
		}
		child.SetOwnerReferences(newOwnerRefs)

		updatedData, err := b.store.MarshalResource(child)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal updated child resource")
		}
		ops = append(ops, clientv3.OpPut(childKey.String(), updatedData))
	}

	deletionKey := fmt.Sprintf("/deletion%s", key.String())
	ops = append(ops, clientv3.OpDelete(deletionKey))

	return ops, nil
}

func (b *DeletionOpBuilder) ListDeletions(ctx context.Context, batchLimit int) ([]types.DeletionRecord, error) {
	prefix := "/deletion/"
	kvs, err := b.store.ListRaw(ctx, prefix, batchLimit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list deletion records from etcd")
	}

	var deletionRecords []types.DeletionRecord
	for _, kv := range kvs {
		var deletionRecord types.DeletionRecord
		if err := json.Unmarshal(kv.Value, &deletionRecord); err != nil {
			continue
		}
		deletionRecords = append(deletionRecords, deletionRecord)
	}

	return deletionRecords, nil
}

func (b *DeletionOpBuilder) AcquireDeletions(ctx context.Context, lockKey string, lockExp time.Duration, batchLimit int) (*types.DeletionBatch, error) {
	leaseResp, err := b.store.GrantLease(ctx, int64(lockExp.Seconds()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to grant lease for deletion lock")
	}

	deletionRecords, err := b.ListDeletions(ctx, batchLimit)
	if err != nil {
		b.store.RevokeLease(ctx, leaseResp.ID)
		return nil, err
	}

	if len(deletionRecords) == 0 {
		b.store.RevokeLease(ctx, leaseResp.ID)
		return &types.DeletionBatch{ObjectKeys: []types.ObjectKey{}, LeaseID: leaseResp.ID}, nil
	}

	var objectKeys []types.ObjectKey

	for _, deletionRecord := range deletionRecords {
		var txnResp *clientv3.TxnResponse
		var err error

		objectLockPath := fmt.Sprintf("/locks/deletion/%s", deletionRecord.ObjectKey.String())

		noLockCompareOp := []clientv3.Cmp{clientv3.Compare(clientv3.Version(objectLockPath), "=", 0)}
		leaseOp := []clientv3.Op{clientv3.OpPut(objectLockPath, lockKey, clientv3.WithLease(leaseResp.ID))}

		txnResp, err = b.store.ExecuteConditionalTransaction(ctx, noLockCompareOp, leaseOp, nil)
		if err == nil && txnResp.Succeeded {
			objectKeys = append(objectKeys, deletionRecord.ObjectKey)
			continue
		}

		lockedByItselfCompareOp := []clientv3.Cmp{clientv3.Compare(clientv3.Value(objectLockPath), "=", lockKey)}

		txnResp, err = b.store.ExecuteConditionalTransaction(ctx, lockedByItselfCompareOp, leaseOp, nil)
		if err == nil && txnResp.Succeeded {
			objectKeys = append(objectKeys, deletionRecord.ObjectKey)
		}
	}

	return &types.DeletionBatch{ObjectKeys: objectKeys, LeaseID: leaseResp.ID}, nil
}
