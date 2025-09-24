package repository

import (
	"context"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type ResourceRepository interface {
	Replace(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error
	Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error)
	List(ctx context.Context, key types.ResourceKey, limit int) ([]*unstructured.Unstructured, error)
	Delete(ctx context.Context, key types.ObjectKey) error
	Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- types.WatchEvent) error
	MarkDeleted(ctx context.Context, key types.ObjectKey) error
	ListDeletions(ctx context.Context, lockKey string, lockExp time.Duration, batchLimit int) (*types.DeletionBatch, error)
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

func (r *resourceRepository) Replace(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error {
	oldResource, err := r.store.Get(ctx, key)
	if err != nil && !types.IsNotFoundError(err) {
		return err
	}

	var oldOwnerRefs []metav1.OwnerReference
	if oldResource != nil {
		oldOwnerRefs = oldResource.GetOwnerReferences()
	}

	diff := types.CalculateOwnerReferenceDiff(
		oldOwnerRefs,
		resource.GetOwnerReferences(),
	)

	var ops []clientv3.Op

	setOp, err := r.store.BuildPutTxOp(key, resource)
	if err != nil {
		return err
	}
	ops = append(ops, setOp)

	if len(diff.Deleted) > 0 || len(diff.Created) > 0 {
		refOps, err := r.ownerRefOpBuilder.BuildDiffOperations(ctx, diff, key)
		if err != nil {
			return errors.Wrap(err, "failed to create reference transaction operations")
		}
		ops = append(ops, refOps...)
	}

	return r.clientWrapper.ExecuteTransaction(ctx, ops)
}

func (r *resourceRepository) Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error) {
	return r.store.Get(ctx, key)
}

func (r *resourceRepository) List(ctx context.Context, key types.ResourceKey, limit int) ([]*unstructured.Unstructured, error) {
	return r.store.List(ctx, key, limit)
}

// TODO: allow deleting only if lock is still valid
func (r *resourceRepository) Delete(ctx context.Context, key types.ObjectKey) error {
	resource, err := r.store.Get(ctx, key)
	if err != nil {
		return err
	}

	ownerRefs := resource.GetOwnerReferences()

	refOps, err := r.ownerRefOpBuilder.BuildDropOperations(ctx, ownerRefs, key)
	if err != nil {
		return errors.Wrap(err, "failed to create owner reference deletion operations")
	}

	childResources, err := r.ownerRefOpBuilder.QueryChildResources(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to query child resources")
	}

	childrenCleanupOps, err := r.deletionOpBuilder.BuildChildrenCleanupOperations(
		resource,
		childResources,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create children cleanup operations")
	}

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpDelete(key.String()))
	ops = append(ops, refOps...)
	ops = append(ops, childrenCleanupOps...)

	return r.clientWrapper.ExecuteTransaction(ctx, ops)
}

func (r *resourceRepository) Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- types.WatchEvent) error {
	watchChan := r.watchManager.Watch(ctx, key)
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
func (r *resourceRepository) MarkDeleted(ctx context.Context, key types.ObjectKey) error {
	resource, err := r.store.Get(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to get resource for deletion marking")
	}

	if resource.GetDeletionTimestamp() != nil {
		return nil // Already marked for deletion
	}

	now := metav1.NewTime(time.Now())
	resource.SetDeletionTimestamp(&now)

	deletionOp, err := r.deletionOpBuilder.BuildMarkDeletionOperation(key)
	if err != nil {
		return errors.Wrap(err, "failed to build mark deletion operations")
	}

	updateOp, err := r.store.BuildPutTxOp(key, resource)
	if err != nil {
		return internalerrors.NewMarshalingError("failed to build set operation for resource with deletion timestamp")
	}

	return r.clientWrapper.ExecuteTransaction(ctx, []clientv3.Op{*deletionOp, updateOp})
}

// ListDeletions returns a batch of resources marked for deletion using distributed locking
func (r *resourceRepository) ListDeletions(ctx context.Context, lockKey string, lockExp time.Duration, batchLimit int) (*types.DeletionBatch, error) {
	return r.deletionOpBuilder.AcquireDeletions(ctx, lockKey, lockExp, batchLimit)
}
