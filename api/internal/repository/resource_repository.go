package repository

import (
	"context"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type ResourceRepository interface {
	Replace(ctx context.Context, obj *sdkmeta.Object) error
	Get(ctx context.Context, key sdkmeta.ObjectKey) (*sdkmeta.Object, error)
	List(ctx context.Context, objType *sdkmeta.ObjectType, limit int) ([]*sdkmeta.Object, error)
	Delete(ctx context.Context, key sdkmeta.ObjectKey) error
	Watch(ctx context.Context, objType *sdkmeta.ObjectType, eventChan chan<- types.WatchEvent) error
	MarkDeleted(ctx context.Context, key sdkmeta.ObjectKey) error
	ListDeletions(ctx context.Context, lockKey string, lockExp time.Duration, batchLimit int) (*DeletionBatch, error)
}

type resourceRepository struct {
	store             ResourceStore
	clientWrapper     ClientWrapper
	ownerRefOpBuilder *OwnerReferenceOpBuilder
	deletionOpBuilder *DeletionOpBuilder
	watchManager      *WatchManager
	logger            *zap.Logger
}

func NewResourceRepository(logger *zap.Logger, store ResourceStore, clientWrapper ClientWrapper, watchConfig WatchConfig, backoffManager *lib.BackoffManager) ResourceRepository {
	ownerRefOpBuilder := NewOwnerReferenceOpBuilder(store, clientWrapper, logger)
	deletionOpBuilder := NewDeletionOpBuilder(store, clientWrapper, logger)
	watchManager := NewWatchManager(store, logger, watchConfig, backoffManager)
	return &resourceRepository{
		store:             store,
		clientWrapper:     clientWrapper,
		ownerRefOpBuilder: ownerRefOpBuilder,
		deletionOpBuilder: deletionOpBuilder,
		watchManager:      watchManager,
		logger:            logger,
	}
}

func (r *resourceRepository) Replace(ctx context.Context, obj *sdkmeta.Object) error {
	oldResource, err := r.store.Get(ctx, *obj.ObjectKey)
	if err != nil && !types.IsNotFoundError(err) {
		return err
	}

	now := time.Now()

	if obj.SystemMeta == nil {
		obj.SystemMeta = &sdkmeta.SystemMeta{
			UID:               "TODO",
			Revision:          "TODO",
			CreationTimestamp: &now,
		}
	}

	putOp, err := r.store.BuildPutTxOp(obj)
	if err != nil {
		return err
	}

	var oldOwnerRefs []sdkmeta.OwnerReference
	if oldResource != nil {
		oldOwnerRefs = oldResource.ObjectMeta.OwnerReferences
	}

	ownerRefOps := r.ownerRefOpBuilder.BuildIndexesUpdateOps(
		*obj.ObjectKey,
		oldOwnerRefs,
		obj.ObjectMeta.OwnerReferences,
	)

	ops := []clientv3.Op{putOp}
	ops = append(ops, ownerRefOps...)

	return r.clientWrapper.ExecuteTransaction(ctx, ops)
}

func (r *resourceRepository) Get(ctx context.Context, key sdkmeta.ObjectKey) (*sdkmeta.Object, error) {
	return r.store.Get(ctx, key)
}

func (r *resourceRepository) List(ctx context.Context, objType *sdkmeta.ObjectType, limit int) ([]*sdkmeta.Object, error) {
	return r.store.List(ctx, objType, limit)
}

// TODO: allow deleting only if lock is still valid
func (r *resourceRepository) Delete(ctx context.Context, key sdkmeta.ObjectKey) error {
	obj, err := r.store.Get(ctx, key)
	if err != nil {
		return err
	}

	ownerReferenceIndexesCleanupOps := r.ownerRefOpBuilder.BuildIndexesCleanupOps(key, obj.ObjectMeta.OwnerReferences)

	childResources, err := r.ownerRefOpBuilder.QueryChildren(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to query children resources")
	}

	childrenCleanupOps, err := r.deletionOpBuilder.BuildChildrenCleanupOps(obj, childResources)
	if err != nil {
		return errors.Wrap(err, "failed to create children cleanup operations")
	}

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpDelete(objectKeyToDbKey(key)))
	ops = append(ops, ownerReferenceIndexesCleanupOps...)
	ops = append(ops, childrenCleanupOps...)

	return r.clientWrapper.ExecuteTransaction(ctx, ops)
}

func (r *resourceRepository) Watch(ctx context.Context, objType *sdkmeta.ObjectType, eventChan chan<- types.WatchEvent) error {
	watchChan := r.watchManager.Watch(ctx, objType, "")
	go func() {
		defer close(eventChan)
		for event := range watchChan {
			select {
			case eventChan <- event:
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// MarkDeleted marks a resource for deletion by setting deletionTimestamp and adding to deletion collection
func (r *resourceRepository) MarkDeleted(ctx context.Context, key sdkmeta.ObjectKey) error {
	resource, err := r.store.Get(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to get resource for deletion marking")
	}

	if resource.SystemMeta.DeletionTimestamp != nil {
		return nil // Already marked for deletion
	}

	now := time.Now()
	resource.SystemMeta.DeletionTimestamp = &now

	deletionOp, err := r.deletionOpBuilder.BuildMarkDeletionOperation(key)
	if err != nil {
		return errors.Wrap(err, "failed to build mark deletion operations")
	}

	updateOp, err := r.store.BuildPutTxOp(resource)
	if err != nil {
		return errors.Wrap(err, "failed to build set operation for resource with deletion timestamp")
	}

	return r.clientWrapper.ExecuteTransaction(ctx, []clientv3.Op{*deletionOp, updateOp})
}

// ListDeletions returns a batch of resources marked for deletion using distributed locking
func (r *resourceRepository) ListDeletions(ctx context.Context, lockKey string, lockExp time.Duration, batchLimit int) (*DeletionBatch, error) {
	return r.deletionOpBuilder.AcquireDeletions(ctx, lockKey, lockExp, batchLimit)
}
