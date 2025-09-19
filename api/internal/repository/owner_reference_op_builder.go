package repository

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type OwnerReferenceOpBuilder struct {
	store ResourceStore
}

// NewOwnerReferenceOpBuilder creates a new OwnerReferenceOpBuilder
func NewOwnerReferenceOpBuilder(store ResourceStore) *OwnerReferenceOpBuilder {
	return &OwnerReferenceOpBuilder{
		store: store,
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

	data, err := b.store.GetRaw(ctx, refKey)
	if err != nil && !IsNotFoundError(err) {
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
