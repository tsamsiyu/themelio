package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type DeletionOpBuilder struct {
	store         ResourceStore
	clientWrapper ClientWrapper
	logger        *zap.Logger
}

func NewDeletionOpBuilder(store ResourceStore, clientWrapper ClientWrapper, logger *zap.Logger) *DeletionOpBuilder {
	return &DeletionOpBuilder{
		store:         store,
		clientWrapper: clientWrapper,
		logger:        logger,
	}
}

func (b *DeletionOpBuilder) BuildMarkDeletionOperation(key sdkmeta.ObjectKey) (*clientv3.Op, error) {
	deletionKey := deletionDbKey(key)
	deletionRecord := types.NewDeletionRecord(key)
	deletionData, err := json.Marshal(deletionRecord)
	if err != nil {
		return nil, internalerrors.NewMarshalingError("failed to marshal deletion record")
	}

	op := clientv3.OpPut(deletionKey, string(deletionData))

	return &op, nil
}

// BuildChildrenCleanupOperations builds all operations needed to clean up children of a resource
func (b *DeletionOpBuilder) BuildChildrenCleanupOperations(
	obj *sdkmeta.Object,
	children []*sdkmeta.Object,
) ([]clientv3.Op, error) {
	var allOps []clientv3.Op

	refCleanupOps, err := b.buildChildrenReferenceCleanupOps(obj, children)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build children reference cleanup operations")
	}
	allOps = append(allOps, refCleanupOps...)

	markDeletionOps, err := b.buildChildrenMarkDeletionOps(obj, children)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build children mark deletion operations")
	}
	allOps = append(allOps, markDeletionOps...)

	return allOps, nil
}

// buildChildrenReferenceCleanupOps builds operations to remove owner references from children
func (b *DeletionOpBuilder) buildChildrenReferenceCleanupOps(
	obj *sdkmeta.Object,
	children []*sdkmeta.Object,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	for _, child := range children {
		hasBlockingParent := hasOtherBlockingReference(child.ObjectMeta.OwnerReferences, obj.SystemMeta.UID)

		if hasBlockingParent {
			newOwnerRefs := removeOwnerReference(child.ObjectMeta.OwnerReferences, obj.SystemMeta.UID)
			child.ObjectMeta.OwnerReferences = newOwnerRefs

			setOp, err := b.store.BuildPutTxOp(child)
			if err != nil {
				return nil, errors.Wrap(err, "failed to build set operation for updated child resource")
			}
			ops = append(ops, setOp)
		}
	}

	return ops, nil
}

// buildChildrenMarkDeletionOps builds operations to mark children for deletion
func (b *DeletionOpBuilder) buildChildrenMarkDeletionOps(
	obj *sdkmeta.Object,
	children []*sdkmeta.Object,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	for _, child := range children {
		hasBlockingParent := hasOtherBlockingReference(child.ObjectMeta.OwnerReferences, obj.SystemMeta.UID)
		if !hasBlockingParent {
			deletionOp, err := b.BuildMarkDeletionOperation(*child.ObjectKey)
			if err != nil {
				return nil, errors.Wrap(err, "failed to build mark deletion operation for child")
			}
			ops = append(ops, *deletionOp)
		}
	}

	return ops, nil
}

func (b *DeletionOpBuilder) ListDeletions(ctx context.Context, batchLimit int) ([]types.DeletionRecord, error) {
	prefix := "/deletion/"
	kvs, err := b.clientWrapper.List(ctx, prefix, batchLimit)
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
	leaseResp, err := b.clientWrapper.GrantLease(ctx, int64(lockExp.Seconds()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to grant lease for deletion lock")
	}

	deletionRecords, err := b.ListDeletions(ctx, batchLimit)
	if err != nil {
		b.clientWrapper.RevokeLease(ctx, leaseResp.ID)
		return nil, err
	}

	if len(deletionRecords) == 0 {
		b.clientWrapper.RevokeLease(ctx, leaseResp.ID)
		return &types.DeletionBatch{ObjectKeys: []sdkmeta.ObjectKey{}, LeaseID: leaseResp.ID}, nil
	}

	var objectKeys []sdkmeta.ObjectKey

	for _, deletionRecord := range deletionRecords {
		var txnResp *clientv3.TxnResponse
		var err error

		objectLockPath := fmt.Sprintf("/locks/deletion/%s", objectKeyToDbKey(deletionRecord.ObjectKey))

		noLockCompareOp := []clientv3.Cmp{clientv3.Compare(clientv3.Version(objectLockPath), "=", 0)}
		leaseOp := []clientv3.Op{clientv3.OpPut(objectLockPath, lockKey, clientv3.WithLease(leaseResp.ID))}

		txnResp, err = b.clientWrapper.ExecuteConditionalTransaction(ctx, noLockCompareOp, leaseOp, nil)
		if err == nil && txnResp.Succeeded {
			objectKeys = append(objectKeys, deletionRecord.ObjectKey)
			continue
		}

		lockedByItselfCompareOp := []clientv3.Cmp{clientv3.Compare(clientv3.Value(objectLockPath), "=", lockKey)}

		txnResp, err = b.clientWrapper.ExecuteConditionalTransaction(ctx, lockedByItselfCompareOp, leaseOp, nil)
		if err == nil && txnResp.Succeeded {
			objectKeys = append(objectKeys, deletionRecord.ObjectKey)
		}
	}

	return &types.DeletionBatch{ObjectKeys: objectKeys, LeaseID: leaseResp.ID}, nil
}

func removeOwnerReference(ownerRefs []sdkmeta.OwnerReference, exceptID string) []sdkmeta.OwnerReference {
	var newOwnerRefs []sdkmeta.OwnerReference
	for _, ownerRef := range ownerRefs {
		if ownerRef.UID == exceptID {
			continue
		}
		newOwnerRefs = append(newOwnerRefs, ownerRef)
	}

	return newOwnerRefs
}

func hasOtherBlockingReference(childOwnerRefs []sdkmeta.OwnerReference, exceptID string) bool {
	for _, ownerRef := range childOwnerRefs {
		if ownerRef.UID == exceptID {
			continue
		}
		if ownerRef.BlockOwnerDeletion {
			return true
		}
	}

	return false
}
