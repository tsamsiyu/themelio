package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type DeletionBatch struct {
	ObjectKeys []sdkmeta.ObjectKey
	LeaseID    clientv3.LeaseID
}

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
	op := clientv3.OpPut(deletionKey, time.Now().Format(time.RFC3339))
	return &op, nil
}

func (b *DeletionOpBuilder) BuildChildrenCleanupOps(
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

func (b *DeletionOpBuilder) ListDeletions(ctx context.Context, batchLimit int) (map[sdkmeta.ObjectKey]time.Time, error) {
	prefix := "/deletion/"
	batch, err := b.clientWrapper.List(ctx, Paging{Prefix: prefix, Limit: batchLimit})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list deletion records from etcd")
	}

	records := make(map[sdkmeta.ObjectKey]time.Time)
	for _, kv := range batch.KVs {
		key, err := parseDeletionKey(kv.Key)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse deletion key")
		}
		value, err := time.Parse(time.RFC3339, string(kv.Value))
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse deletion record")
		}
		records[key] = value
	}

	return records, nil
}

func (b *DeletionOpBuilder) AcquireDeletions(ctx context.Context, lockKey string, lockExp time.Duration, batchLimit int) (*DeletionBatch, error) {
	leaseResp, err := b.clientWrapper.GrantLease(ctx, int64(lockExp.Seconds()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to grant lease for deletion lock")
	}

	deletions, err := b.ListDeletions(ctx, batchLimit)
	if err != nil {
		b.clientWrapper.RevokeLease(ctx, leaseResp.ID)
		return nil, err
	}

	if len(deletions) == 0 {
		b.clientWrapper.RevokeLease(ctx, leaseResp.ID)
		return &DeletionBatch{ObjectKeys: []sdkmeta.ObjectKey{}, LeaseID: leaseResp.ID}, nil
	}

	var objectKeys []sdkmeta.ObjectKey

	for key := range deletions {
		txn := b.clientWrapper.Client().Txn(ctx)
		deletionLockDbKey := deletionLockDbKey(key)

		ifLockDoesNotExistOp := []clientv3.Cmp{clientv3.Compare(clientv3.Version(deletionLockDbKey), "=", 0)}
		ifLockedByItselfOp := []clientv3.Cmp{clientv3.Compare(clientv3.Value(deletionLockDbKey), "=", lockKey)}
		leaseOp := []clientv3.Op{clientv3.OpPut(deletionLockDbKey, lockKey, clientv3.WithLease(leaseResp.ID))}

		txnResp, err := txn.If(ifLockDoesNotExistOp...).Then(leaseOp...).Commit()
		if err != nil {
			return nil, errors.Wrap(err, "failed to acquire deletion lock")
		}
		if txnResp.Succeeded {
			objectKeys = append(objectKeys, key)
			continue
		}

		txnResp, err = txn.If(ifLockedByItselfOp...).Then(leaseOp...).Commit()
		if err != nil {
			return nil, errors.Wrap(err, "failed to acquire deletion lock")
		}
		if txnResp.Succeeded {
			objectKeys = append(objectKeys, key)
		}
	}

	return &DeletionBatch{ObjectKeys: objectKeys, LeaseID: leaseResp.ID}, nil
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

func deletionDbKey(key sdkmeta.ObjectKey) string {
	return fmt.Sprintf("/deletion%s", objectKeyToDbKey(key))
}

func deletionLockDbKey(key sdkmeta.ObjectKey) string {
	return fmt.Sprintf("/deletion-lock/%s", objectKeyToDbKey(key))
}

func parseDeletionKey(key string) (sdkmeta.ObjectKey, error) {
	key = strings.TrimPrefix(key, "/deletion")
	return parseObjectKey(key)
}
