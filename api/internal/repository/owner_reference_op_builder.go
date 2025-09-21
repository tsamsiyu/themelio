package repository

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

// the idea of reversed references is to quickly find children resources
// we don't need reversed references for owner references that are not blocking deletion

type OwnerReferenceOpBuilder struct {
	store         ResourceStore
	clientWrapper ClientWrapper
	logger        *zap.Logger
}

// NewOwnerReferenceOpBuilder creates a new OwnerReferenceOpBuilder
func NewOwnerReferenceOpBuilder(store ResourceStore, clientWrapper ClientWrapper, logger *zap.Logger) *OwnerReferenceOpBuilder {
	return &OwnerReferenceOpBuilder{
		store:         store,
		clientWrapper: clientWrapper,
		logger:        logger,
	}
}

// BuildDiffOperations builds operations for owner reference diff changes
func (b *OwnerReferenceOpBuilder) BuildDiffOperations(
	ctx context.Context,
	diff types.OwnerReferenceDiff,
	childKey types.ObjectKey,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	if len(diff.Deleted) > 0 {
		deletedOps, err := b.BuildDropOperations(ctx, diff.Deleted, childKey)
		if err != nil {
			return nil, err
		}
		ops = append(ops, deletedOps...)
	}

	if len(diff.Created) > 0 {
		createdOps, err := b.BuildAddOperations(ctx, diff.Created, childKey)
		if err != nil {
			return nil, err
		}
		ops = append(ops, createdOps...)
	}

	return ops, nil
}

// BuildDropOperations builds operations to drop owner references
func (b *OwnerReferenceOpBuilder) BuildDropOperations(
	ctx context.Context,
	ownerRefs []metav1.OwnerReference,
	childKey types.ObjectKey,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	for _, ownerRef := range ownerRefs {
		parentKey := types.OwnerRefToObjectKey(ownerRef, childKey.Namespace)
		refKey := fmt.Sprintf("/ref%s", parentKey.String())

		currentChildKeys, err := b.GetReversedOwnerReferences(ctx, parentKey)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get current children for parent %s", parentKey.String())
		}

		currentChildKeys.Delete(childKey.String())

		if len(currentChildKeys) == 0 {
			ops = append(ops, clientv3.OpDelete(refKey))
		} else {
			ops = append(ops, clientv3.OpPut(refKey, currentChildKeys.Encode()))
		}
	}

	return ops, nil
}

// BuildAddOperations builds operations to add owner references
func (b *OwnerReferenceOpBuilder) BuildAddOperations(
	ctx context.Context,
	ownerRefs []metav1.OwnerReference,
	childKey types.ObjectKey,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	for _, ownerRef := range ownerRefs {
		if ownerRef.BlockOwnerDeletion != nil && !*ownerRef.BlockOwnerDeletion {
			continue
		}

		parentKey := types.OwnerRefToObjectKey(ownerRef, childKey.Namespace)
		refKey := fmt.Sprintf("/ref%s", parentKey.String())

		currentChildKeys, err := b.GetReversedOwnerReferences(ctx, parentKey)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get current children for parent %s", parentKey.String())
		}

		currentChildKeys.Put(childKey.String())
		ops = append(ops, clientv3.OpPut(refKey, currentChildKeys.Encode()))
	}

	return ops, nil
}

// GetReversedOwnerReferences gets the current reversed owner references for a parent key
func (b *OwnerReferenceOpBuilder) GetReversedOwnerReferences(
	ctx context.Context,
	parentKey types.ObjectKey,
) (types.ReversedOwnerReferenceSet, error) {
	refKey := fmt.Sprintf("/ref%s", parentKey.String())

	data, err := b.clientWrapper.Get(ctx, refKey)
	if err != nil && !types.IsNotFoundError(err) {
		return nil, errors.Wrap(err, "failed to get owner references from etcd")
	}

	childKeys := types.NewReversedOwnerReferenceSet()
	if len(data) > 0 {
		childKeys.Decode(string(data))
	}

	return childKeys, nil
}

// BuildDeletionOperations builds operations to remove references for a list of object keys
func (b *OwnerReferenceOpBuilder) BuildDeletionOperations(
	ctx context.Context,
	removeReferencesObjectKeys []types.ObjectKey,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	for _, objectKey := range removeReferencesObjectKeys {
		refKey := fmt.Sprintf("/ref%s", objectKey.String())
		ops = append(ops, clientv3.OpDelete(refKey))
	}

	return ops, nil
}

// QueryChildResources queries all child resources for a given parent key
func (b *OwnerReferenceOpBuilder) QueryChildResources(
	ctx context.Context,
	parentKey types.ObjectKey,
) (map[string]*unstructured.Unstructured, error) {
	reversedOwnerRefs, err := b.GetReversedOwnerReferences(ctx, parentKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get reversed owner references")
	}

	childResources := make(map[string]*unstructured.Unstructured)

	for childKeyStr := range reversedOwnerRefs {
		childKey, err := types.ParseObjectKey(childKeyStr)
		if err != nil {
			b.logger.Error("Failed to parse child object key",
				zap.String("childKey", childKeyStr),
				zap.String("parentKey", parentKey.String()),
				zap.Error(err))
			continue
		}

		child, err := b.store.Get(ctx, childKey)
		if err != nil {
			b.logger.Error("Failed to get child resource",
				zap.String("childKey", childKeyStr),
				zap.String("parentKey", parentKey.String()),
				zap.Error(err))
			continue
		}

		if child == nil {
			b.logger.Warn("child resource not found by reversed reference",
				zap.String("childKey", childKeyStr),
				zap.String("parentKey", parentKey.String()))
			continue
		}

		childResources[childKeyStr] = child
	}

	return childResources, nil
}
