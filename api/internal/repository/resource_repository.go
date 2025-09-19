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

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type WatchEventType string

const (
	WatchEventTypeAdded    WatchEventType = "added"
	WatchEventTypeModified WatchEventType = "modified"
	WatchEventTypeDeleted  WatchEventType = "deleted"
)

type WatchEvent struct {
	Type      WatchEventType             `json:"type"`
	Object    *unstructured.Unstructured `json:"object"`
	Timestamp time.Time                  `json:"timestamp"`
}

type ResourceRepository interface {
	Replace(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error
	Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error)
	List(ctx context.Context, key types.ResourceKey) ([]*unstructured.Unstructured, error)
	Delete(ctx context.Context, key types.ObjectKey, markDeletionObjectKeys []types.ObjectKey, removeReferencesObjectKeys []types.ObjectKey) error
	Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- WatchEvent) error
	MarkDeleted(ctx context.Context, key types.ObjectKey) error
	ListDeletions(ctx context.Context) ([]types.ObjectKey, error)
	GetReversedOwnerReferences(ctx context.Context, parentKey types.ObjectKey) (types.ReversedOwnerReferenceSet, error)
}

type resourceRepository struct {
	store  ResourceStore
	logger *zap.Logger
}

func NewResourceRepository(logger *zap.Logger, client *clientv3.Client) ResourceRepository {
	store := NewResourceStore(logger, client)
	return &resourceRepository{
		store:  store,
		logger: logger,
	}
}

func (r *resourceRepository) Replace(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error {
	// Get old resource to calculate owner reference diff
	oldResource, err := r.store.Get(ctx, key)
	if err != nil {
		return err
	}

	diff := types.CalculateOwnerReferenceDiff(
		oldResource.GetOwnerReferences(),
		resource.GetOwnerReferences(),
	)

	// Build transaction operations
	var ops []clientv3.Op

	// Add resource update operation
	data, err := r.marshalResource(resource)
	if err != nil {
		return err
	}
	ops = append(ops, clientv3.OpPut(key.String(), data))

	// Add owner reference operations if there are changes
	if len(diff.Deleted) > 0 || len(diff.Created) > 0 {
		refOps, err := r.createTxOpsToUpdateReversedOwnerReferences(ctx, diff, key)
		if err != nil {
			return errors.Wrap(err, "failed to create reference transaction operations")
		}
		ops = append(ops, refOps...)
	}

	// Execute transaction
	return r.store.ExecuteTransaction(ctx, ops)
}

func (r *resourceRepository) Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error) {
	return r.store.Get(ctx, key)
}

func (r *resourceRepository) List(ctx context.Context, key types.ResourceKey) ([]*unstructured.Unstructured, error) {
	return r.store.List(ctx, key)
}

func (r *resourceRepository) Delete(ctx context.Context, key types.ObjectKey, markDeletionObjectKeys []types.ObjectKey, removeReferencesObjectKeys []types.ObjectKey) error {
	etcdKey := key.String()

	resource, err := r.Get(ctx, key)
	if err != nil {
		return err
	}

	ownerRefs := resource.GetOwnerReferences()
	if len(ownerRefs) == 0 {
		return r.store.Delete(ctx, key)
	}

	refOps, err := r.createTxOpsToDropReversedOwnerReferences(ctx, ownerRefs, key)
	if err != nil {
		return errors.Wrap(err, "failed to create owner reference deletion operations")
	}

	// TODO: remove from "/deletion/"

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpDelete(etcdKey))
	ops = append(ops, refOps...)

	for _, childKey := range markDeletionObjectKeys {
		deletionKey := fmt.Sprintf("/deletion%s", childKey.String())
		deletionRecord := map[string]interface{}{
			"timestamp": time.Now(),
		}
		deletionData, err := json.Marshal(deletionRecord)
		if err != nil {
			return errors.Wrap(err, "failed to marshal deletion record")
		}
		ops = append(ops, clientv3.OpPut(deletionKey, string(deletionData)))
	}

	for _, childKey := range removeReferencesObjectKeys {
		child, err := r.Get(ctx, childKey)
		if err != nil {
			return errors.Wrap(err, "failed to get child for owner reference removal")
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

		updatedData, err := r.marshalResource(child)
		if err != nil {
			return errors.Wrap(err, "failed to marshal updated child resource")
		}
		ops = append(ops, clientv3.OpPut(childKey.String(), updatedData))
	}

	deletionKey := fmt.Sprintf("/deletion%s", etcdKey)
	ops = append(ops, clientv3.OpDelete(deletionKey))

	// Execute transaction
	return r.store.ExecuteTransaction(ctx, ops)
}

func (r *resourceRepository) marshalResource(resource *unstructured.Unstructured) (string, error) {
	data, err := json.Marshal(resource)
	if err != nil {
		return "", internalerrors.NewMarshalingError("Failed to marshal resource")
	}
	return string(data), nil
}

func (r *resourceRepository) Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- WatchEvent) error {
	return r.store.Watch(ctx, key, eventChan)
}

func (r *resourceRepository) GetReversedOwnerReferences(
	ctx context.Context,
	parentKey types.ObjectKey,
) (types.ReversedOwnerReferenceSet, error) {
	refKey := fmt.Sprintf("/ref%s", parentKey.String())

	data, err := r.store.GetRaw(ctx, refKey)
	if err != nil && !IsNotFoundError(err) {
		return nil, errors.Wrap(err, "failed to get owner references from etcd")
	}

	childKeys := types.NewReversedOwnerReferenceSet()
	if len(data) > 0 {
		childKeys.Decode(string(data))
	}

	return childKeys, nil
}

// MarkDeleted marks a resource for deletion by setting deletionTimestamp and adding to deletion collection
func (r *resourceRepository) MarkDeleted(ctx context.Context, key types.ObjectKey) error {
	// Get resource
	resource, err := r.store.Get(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to get resource for deletion marking")
	}

	if deletionTimestamp := resource.GetDeletionTimestamp(); deletionTimestamp != nil {
		r.logger.Debug("Resource already marked for deletion",
			zap.Object("objectKey", key),
			zap.Time("deletionTimestamp", deletionTimestamp.Time))
		return nil // Already marked for deletion
	}

	now := metav1.NewTime(time.Now())
	resource.SetDeletionTimestamp(&now)

	// Build transaction operations
	var ops []clientv3.Op

	// Update resource with deletion timestamp
	updatedData, err := r.marshalResource(resource)
	if err != nil {
		return internalerrors.NewMarshalingError("failed to marshal resource with deletion timestamp")
	}
	ops = append(ops, clientv3.OpPut(key.String(), updatedData))

	// Add deletion record
	deletionKey := fmt.Sprintf("/deletion%s", key.String())
	deletionRecord := map[string]interface{}{
		"objectKey": key,
		"timestamp": now.Time,
	}
	deletionData, err := json.Marshal(deletionRecord)
	if err != nil {
		return internalerrors.NewMarshalingError("failed to marshal deletion record")
	}
	ops = append(ops, clientv3.OpPut(deletionKey, string(deletionData)))

	// Execute transaction
	return r.store.ExecuteTransaction(ctx, ops)
}

// ListDeletions returns all resources marked for deletion
func (r *resourceRepository) ListDeletions(ctx context.Context) ([]types.ObjectKey, error) {
	prefix := "/deletion/"

	kvs, err := r.store.ListRaw(ctx, prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list deletion records from etcd")
	}

	var objectKeys []types.ObjectKey
	for _, kv := range kvs {
		var deletionRecord map[string]interface{}
		if err := json.Unmarshal(kv.Value, &deletionRecord); err != nil {
			continue
		}

		objectKeyRaw, ok := deletionRecord["objectKey"]
		if !ok {
			continue
		}

		objectKey, err := types.ParseObjectKey(objectKeyRaw.(string))
		if err != nil {
			continue
		}

		objectKeys = append(objectKeys, objectKey)
	}

	return objectKeys, nil
}

func (r *resourceRepository) createTxOpsToUpdateReversedOwnerReferences(
	ctx context.Context,
	diff types.OwnerReferenceDiff,
	childKey types.ObjectKey,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	deletedOps, err := r.createTxOpsToDropReversedOwnerReferences(ctx, diff.Deleted, childKey)
	if err != nil {
		return nil, err
	}
	ops = append(ops, deletedOps...)

	createdOps, err := r.createTxOpsToAddReversedOwnerReferences(ctx, diff.Created, childKey)
	if err != nil {
		return nil, err
	}
	ops = append(ops, createdOps...)

	return ops, nil
}

func (r *resourceRepository) createTxOpsToDropReversedOwnerReferences(
	ctx context.Context,
	ownerRefs []metav1.OwnerReference,
	childKey types.ObjectKey,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	for _, ownerRef := range ownerRefs {
		parentKey := types.OwnerRefToObjectKey(ownerRef, childKey.Namespace)
		refKey := fmt.Sprintf("/ref%s", parentKey.String())

		currentChildKeys, err := r.GetReversedOwnerReferences(ctx, parentKey)
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

func (r *resourceRepository) createTxOpsToAddReversedOwnerReferences(
	ctx context.Context,
	ownerRefs []metav1.OwnerReference,
	childKey types.ObjectKey,
) ([]clientv3.Op, error) {
	var ops []clientv3.Op

	for _, ownerRef := range ownerRefs {
		parentKey := types.OwnerRefToObjectKey(ownerRef, childKey.Namespace)
		refKey := fmt.Sprintf("/ref%s", parentKey.String())

		currentChildKeys, err := r.GetReversedOwnerReferences(ctx, parentKey)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get current children for parent %s", parentKey.String())
		}

		currentChildKeys.Put(childKey.String())
		ops = append(ops, clientv3.OpPut(refKey, currentChildKeys.Encode()))
	}

	return ops, nil
}
