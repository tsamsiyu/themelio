package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func (b *DeletionOpBuilder) BuildMarkDeletionOperations(
	ctx context.Context,
	key types.ObjectKey,
) ([]clientv3.Op, error) {
	resource, err := b.store.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get resource for deletion marking")
	}

	if deletionTimestamp := resource.GetDeletionTimestamp(); deletionTimestamp != nil {
		return nil, nil // Already marked for deletion
	}

	now := metav1.NewTime(time.Now())
	resource.SetDeletionTimestamp(&now)

	var ops []clientv3.Op

	updatedData, err := b.store.MarshalResource(resource)
	if err != nil {
		return nil, internalerrors.NewMarshalingError("failed to marshal resource with deletion timestamp")
	}
	ops = append(ops, clientv3.OpPut(key.String(), updatedData))

	deletionKey := fmt.Sprintf("/deletion%s", key.String())
	deletionRecord := types.NewDeletionRecord(key)
	deletionData, err := json.Marshal(deletionRecord)
	if err != nil {
		return nil, internalerrors.NewMarshalingError("failed to marshal deletion record")
	}
	ops = append(ops, clientv3.OpPut(deletionKey, string(deletionData)))

	return ops, nil
}

func (b *DeletionOpBuilder) BuildDeletionOperations(
	ctx context.Context,
	key types.ObjectKey,
	markDeletionObjectKeys []types.ObjectKey,
	removeReferencesObjectKeys []types.ObjectKey,
) ([]clientv3.Op, error) {
	resource, err := b.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	ownerRefs := resource.GetOwnerReferences()
	if len(ownerRefs) == 0 {
		return []clientv3.Op{clientv3.OpDelete(key.String())}, nil
	}

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpDelete(key.String()))

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

func (b *DeletionOpBuilder) ListDeletions(ctx context.Context) ([]types.ObjectKey, error) {
	prefix := "/deletion/"

	kvs, err := b.store.ListRaw(ctx, prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list deletion records from etcd")
	}

	var objectKeys []types.ObjectKey
	for _, kv := range kvs {
		var deletionRecord types.DeletionRecord
		if err := json.Unmarshal(kv.Value, &deletionRecord); err != nil {
			continue
		}

		objectKeys = append(objectKeys, deletionRecord.ObjectKey)
	}

	return objectKeys, nil
}
