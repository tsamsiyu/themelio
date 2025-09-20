package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metaTypes "k8s.io/apimachinery/pkg/types"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type DeletionOpBuilder struct {
	store  ResourceStore
	logger *zap.Logger
}

func NewDeletionOpBuilder(store ResourceStore, logger *zap.Logger) *DeletionOpBuilder {
	return &DeletionOpBuilder{
		store:  store,
		logger: logger,
	}
}

func (b *DeletionOpBuilder) BuildMarkDeletionOperation(
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

// BuildChildrenCleanupOperations builds all operations needed to clean up children of a resource
func (b *DeletionOpBuilder) BuildChildrenCleanupOperations(
	resource *unstructured.Unstructured,
	childResources map[string]*unstructured.Unstructured,
) ([]clientv3.Op, error) {
	var allOps []clientv3.Op

	refCleanupOps, err := b.buildChildrenReferenceCleanupOps(resource, childResources)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build children reference cleanup operations")
	}
	allOps = append(allOps, refCleanupOps...)

	markDeletionOps, err := b.buildChildrenMarkDeletionOps(resource, childResources)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build children mark deletion operations")
	}
	allOps = append(allOps, markDeletionOps...)

	return allOps, nil
}

// buildChildrenReferenceCleanupOps builds operations to remove owner references from children
func (b *DeletionOpBuilder) buildChildrenReferenceCleanupOps(
	resource *unstructured.Unstructured,
	childResources map[string]*unstructured.Unstructured,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	for childKeyStr, child := range childResources {
		childKey, err := types.ParseObjectKey(childKeyStr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse child key")
		}

		childOwnerRefs := child.GetOwnerReferences()
		hasBlockingParent := hasOtherBlockingReference(childOwnerRefs, resource.GetUID())

		if hasBlockingParent {
			newOwnerRefs := removeOwnerReference(childOwnerRefs, resource.GetUID())
			child.SetOwnerReferences(newOwnerRefs)

			updatedData, err := b.store.MarshalResource(child)
			if err != nil {
				return nil, errors.Wrap(err, "failed to marshal updated child resource")
			}
			ops = append(ops, clientv3.OpPut(childKey.String(), updatedData))
		}
	}

	return ops, nil
}

// buildChildrenMarkDeletionOps builds operations to mark children for deletion
func (b *DeletionOpBuilder) buildChildrenMarkDeletionOps(
	resource *unstructured.Unstructured,
	childResources map[string]*unstructured.Unstructured,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	for childKeyStr, child := range childResources {
		childKey, err := types.ParseObjectKey(childKeyStr)
		if err != nil {
			continue
		}

		childOwnerRefs := child.GetOwnerReferences()
		hasBlockingParent := hasOtherBlockingReference(childOwnerRefs, resource.GetUID())

		if !hasBlockingParent {
			deletionOp, err := b.BuildMarkDeletionOperation(childKey)
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

func removeOwnerReference(ownerRefs []metav1.OwnerReference, exceptID metaTypes.UID) []metav1.OwnerReference {
	var newOwnerRefs []metav1.OwnerReference
	for _, ownerRef := range ownerRefs {
		if ownerRef.UID == exceptID {
			continue
		}
		newOwnerRefs = append(newOwnerRefs, ownerRef)
	}

	return newOwnerRefs
}

func hasOtherBlockingReference(childOwnerRefs []metav1.OwnerReference, exceptID metaTypes.UID) bool {
	for _, ownerRef := range childOwnerRefs {
		if ownerRef.UID == exceptID {
			continue
		}
		if ownerRef.BlockOwnerDeletion != nil && *ownerRef.BlockOwnerDeletion {
			return true
		}
	}

	return false
}
