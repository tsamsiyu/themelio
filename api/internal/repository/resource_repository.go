package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type resourceRepository struct {
	store             types.ResourceStore
	clientWrapper     types.ClientWrapper
	ownerRefOpBuilder *OwnerReferenceOpBuilder
	deletionOpBuilder *DeletionOpBuilder
	labelsOpBuilder   *LabelsOperations
	watchManager      *WatchManager
	logger            *zap.Logger
}

func NewResourceRepository(
	logger *zap.Logger,
	store types.ResourceStore,
	clientWrapper types.ClientWrapper,
	watchConfig types.WatchConfig,
	backoffManager *lib.BackoffManager,
) types.ResourceRepository {
	ownerRefOpBuilder := NewOwnerReferenceOpBuilder(store, clientWrapper, logger)
	deletionOpBuilder := NewDeletionOpBuilder(store, clientWrapper, logger)
	labelsOpBuilder := NewLabelsOperations(store, logger)
	watchManager := NewWatchManager(store, logger, watchConfig, backoffManager)
	return &resourceRepository{
		store:             store,
		clientWrapper:     clientWrapper,
		ownerRefOpBuilder: ownerRefOpBuilder,
		deletionOpBuilder: deletionOpBuilder,
		labelsOpBuilder:   labelsOpBuilder,
		watchManager:      watchManager,
		logger:            logger,
	}
}

func (r *resourceRepository) Replace(ctx context.Context, obj *sdkmeta.Object, optimisticLock bool) error {
	oldObj, err := r.store.Get(ctx, *obj.ObjectKey)
	if err != nil && !IsNotFoundError(err) {
		return err
	}

	beforeSave(oldObj, obj)

	putOp, err := r.store.BuildPutTxOp(obj)
	if err != nil {
		return err
	}

	var oldOwnerRefs []sdkmeta.OwnerReference
	if oldObj != nil {
		oldOwnerRefs = oldObj.ObjectMeta.OwnerReferences
	}

	ownerRefOps := r.ownerRefOpBuilder.BuildIndexesUpdateOps(
		*obj.ObjectKey,
		oldOwnerRefs,
		obj.ObjectMeta.OwnerReferences,
	)

	var oldLabels map[string]string
	if oldObj != nil {
		oldLabels = oldObj.ObjectMeta.Labels
	}

	labelsOps := r.labelsOpBuilder.BuildLabelsUpdateOps(
		*obj.ObjectKey,
		oldLabels,
		obj.ObjectMeta.Labels,
	)

	txn := r.clientWrapper.Client().Txn(ctx)

	ops := []clientv3.Op{}
	ops = append(ops, putOp)
	ops = append(ops, ownerRefOps...)
	ops = append(ops, labelsOps...)

	if optimisticLock && oldObj != nil {
		onlyIfOp := clientv3.Compare(clientv3.Value(objectKeyToDbKey(*obj.ObjectKey)), "=", fmt.Sprintf("%d", obj.SystemMeta.Version))
		_, err = txn.If(onlyIfOp).Then(ops...).Commit()
		return err
	}

	_, err = txn.Then(ops...).Commit()
	return err
}

func (r *resourceRepository) Get(ctx context.Context, key sdkmeta.ObjectKey) (*sdkmeta.Object, error) {
	return r.store.Get(ctx, key)
}

func (r *resourceRepository) List(ctx context.Context, objType *sdkmeta.ObjectType) ([]*sdkmeta.Object, error) {
	res, err := r.store.List(ctx, objType, nil)
	if err != nil {
		return nil, err
	}
	return res.Objects, nil
}

func (r *resourceRepository) Delete(ctx context.Context, key sdkmeta.ObjectKey, lockValue string) error {
	obj, err := r.store.Get(ctx, key)
	if err != nil {
		return err
	}

	childrenReferencesClenaupOps := r.ownerRefOpBuilder.BuildIndexesCleanupOps(key, obj.ObjectMeta.OwnerReferences)

	labelsCleanupOps := r.labelsOpBuilder.BuildLabelsCleanupOps(key, obj.ObjectMeta.Labels)

	childResources, err := r.ownerRefOpBuilder.QueryChildren(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to query children resources")
	}

	childrenCleanupOps, err := r.deletionOpBuilder.BuildChildrenCleanupOps(obj, childResources)
	if err != nil {
		return errors.Wrap(err, "failed to create children cleanup operations")
	}

	ifLockedByItselfOp := clientv3.Compare(clientv3.Value(deletionLockDbKey(key)), "=", lockValue)

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpDelete(objectKeyToDbKey(key)))
	ops = append(ops, clientv3.OpDelete(deletionLockDbKey(key)))
	ops = append(ops, childrenReferencesClenaupOps...)
	ops = append(ops, labelsCleanupOps...)
	ops = append(ops, childrenCleanupOps...)

	txn := r.clientWrapper.Client().Txn(ctx)
	_, err = txn.If(ifLockedByItselfOp).Then(ops...).Commit()
	return err
}

func (r *resourceRepository) Watch(ctx context.Context, objType *sdkmeta.ObjectType, revision int64) (<-chan types.WatchEvent, error) {
	return r.watchManager.Watch(ctx, objType, revision)
}

// MarkDeleted marks a resource for deletion by setting deletionTimestamp and adding to deletion collection
func (r *resourceRepository) MarkDeleted(ctx context.Context, key sdkmeta.ObjectKey) error {
	resource, err := r.store.Get(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to get resource for deletion marking")
	}

	if resource.SystemMeta.DeletionTime != nil {
		return nil // Already marked for deletion
	}

	now := time.Now()
	resource.SystemMeta.DeletionTime = &now

	deletionOp, err := r.deletionOpBuilder.BuildMarkDeletionOperation(key)
	if err != nil {
		return errors.Wrap(err, "failed to build mark deletion operations")
	}

	updateOp, err := r.store.BuildPutTxOp(resource)
	if err != nil {
		return errors.Wrap(err, "failed to build set operation for resource with deletion timestamp")
	}

	txn := r.clientWrapper.Client().Txn(ctx)
	_, err = txn.Then([]clientv3.Op{*deletionOp, updateOp}...).Commit()
	return err
}

// ListDeletions returns a batch of resources marked for deletion using distributed locking
func (r *resourceRepository) ListDeletions(ctx context.Context, lockKey string, lockExp time.Duration, batchLimit int) (*types.DeletionBatch, error) {
	return r.deletionOpBuilder.AcquireDeletions(ctx, lockKey, lockExp, batchLimit)
}

func beforeSave(oldResource *sdkmeta.Object, newResource *sdkmeta.Object) {
	now := time.Now()

	if oldResource == nil {
		newResource.SystemMeta = &sdkmeta.SystemMeta{
			CreationTime: &now,
		}
	} else {
		newResource.SystemMeta.LastUpdateTime = &now
	}
}
