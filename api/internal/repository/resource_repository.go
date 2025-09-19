package repository

import (
	"context"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type ResourceRepository interface {
	Replace(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error
	Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error)
	List(ctx context.Context, key types.ResourceKey) ([]*unstructured.Unstructured, error)
	Delete(ctx context.Context, key types.ObjectKey, markDeletionObjectKeys []types.ObjectKey, removeReferencesObjectKeys []types.ObjectKey) error
	Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- types.WatchEvent) error
	MarkDeleted(ctx context.Context, key types.ObjectKey) error
	ListDeletions(ctx context.Context) ([]types.ObjectKey, error)
	GetReversedOwnerReferences(ctx context.Context, parentKey types.ObjectKey) (types.ReversedOwnerReferenceSet, error)
}

type resourceRepository struct {
	store             ResourceStore
	ownerRefOpBuilder *OwnerReferenceOpBuilder
	deletionOpBuilder *DeletionOpBuilder
	logger            *zap.Logger
}

func NewResourceRepository(logger *zap.Logger, client *clientv3.Client) ResourceRepository {
	store := NewResourceStore(logger, client)
	ownerRefOpBuilder := NewOwnerReferenceOpBuilder(store)
	deletionOpBuilder := NewDeletionOpBuilder(store)
	return &resourceRepository{
		store:             store,
		ownerRefOpBuilder: ownerRefOpBuilder,
		deletionOpBuilder: deletionOpBuilder,
		logger:            logger,
	}
}

func (r *resourceRepository) Replace(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error {
	oldResource, err := r.store.Get(ctx, key)
	if err != nil {
		return err
	}

	diff := types.CalculateOwnerReferenceDiff(
		oldResource.GetOwnerReferences(),
		resource.GetOwnerReferences(),
	)

	var ops []clientv3.Op

	data, err := r.store.MarshalResource(resource)
	if err != nil {
		return err
	}
	ops = append(ops, clientv3.OpPut(key.String(), data))

	if len(diff.Deleted) > 0 || len(diff.Created) > 0 {
		refOps, err := r.ownerRefOpBuilder.BuildDiffOperations(ctx, diff, key)
		if err != nil {
			return errors.Wrap(err, "failed to create reference transaction operations")
		}
		ops = append(ops, refOps...)
	}

	return r.store.ExecuteTransaction(ctx, ops)
}

func (r *resourceRepository) Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error) {
	return r.store.Get(ctx, key)
}

func (r *resourceRepository) List(ctx context.Context, key types.ResourceKey) ([]*unstructured.Unstructured, error) {
	return r.store.List(ctx, key)
}

func (r *resourceRepository) Delete(ctx context.Context, key types.ObjectKey, markDeletionObjectKeys []types.ObjectKey, removeReferencesObjectKeys []types.ObjectKey) error {
	resource, err := r.Get(ctx, key)
	if err != nil {
		return err
	}

	ownerRefs := resource.GetOwnerReferences()
	if len(ownerRefs) == 0 {
		return r.store.Delete(ctx, key)
	}

	refOps, err := r.ownerRefOpBuilder.BuildDropOperations(ctx, ownerRefs, key)
	if err != nil {
		return errors.Wrap(err, "failed to create owner reference deletion operations")
	}

	deletionOps, err := r.deletionOpBuilder.BuildDeletionOperations(ctx, key, markDeletionObjectKeys, removeReferencesObjectKeys)
	if err != nil {
		return errors.Wrap(err, "failed to create deletion operations")
	}

	var ops []clientv3.Op
	ops = append(ops, refOps...)
	ops = append(ops, deletionOps...)

	return r.store.ExecuteTransaction(ctx, ops)
}

func (r *resourceRepository) Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- types.WatchEvent) error {
	return r.store.Watch(ctx, key, eventChan)
}

func (r *resourceRepository) GetReversedOwnerReferences(
	ctx context.Context,
	parentKey types.ObjectKey,
) (types.ReversedOwnerReferenceSet, error) {
	return r.ownerRefOpBuilder.GetReversedOwnerReferences(ctx, parentKey)
}

// MarkDeleted marks a resource for deletion by setting deletionTimestamp and adding to deletion collection
func (r *resourceRepository) MarkDeleted(ctx context.Context, key types.ObjectKey) error {
	ops, err := r.deletionOpBuilder.BuildMarkDeletionOperations(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to build mark deletion operations")
	}

	if len(ops) == 0 {
		r.logger.Debug("Resource already marked for deletion", zap.Object("objectKey", key))
		return nil // Already marked for deletion
	}

	return r.store.ExecuteTransaction(ctx, ops)
}

// ListDeletions returns all resources marked for deletion
func (r *resourceRepository) ListDeletions(ctx context.Context) ([]types.ObjectKey, error) {
	return r.deletionOpBuilder.ListDeletions(ctx)
}
