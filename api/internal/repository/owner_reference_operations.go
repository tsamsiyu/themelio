package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

// todo: use parent key + child key as the key and empty string as a value

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

func (b *OwnerReferenceOpBuilder) BuildIndexesUpdateOps(
	objKey sdkmeta.ObjectKey,
	oldOwnerRefs []sdkmeta.OwnerReference,
	newOwnerRefs []sdkmeta.OwnerReference,
) []clientv3.Op {
	var ops []clientv3.Op

	deleted, created := CalculateOwnerReferenceDiff(oldOwnerRefs, newOwnerRefs)

	for _, ownerRef := range deleted {
		parentKey := ownerRef.ToObjectKey()
		dbKey := buildOwnerReferenceIndexDbKey(parentKey, objKey)
		ops = append(ops, clientv3.OpDelete(dbKey))
	}

	for _, ownerRef := range created {
		parentKey := ownerRef.ToObjectKey()
		dbKey := buildOwnerReferenceIndexDbKey(parentKey, objKey)
		ops = append(ops, clientv3.OpPut(dbKey, ""))
	}

	return ops
}

func (b *OwnerReferenceOpBuilder) BuildIndexesCleanupOps(objKey sdkmeta.ObjectKey, ownerRefs []sdkmeta.OwnerReference) []clientv3.Op {
	var ops []clientv3.Op
	for _, ownerRef := range ownerRefs {
		parentKey := ownerRef.ToObjectKey()
		dbKey := buildOwnerReferenceIndexDbKey(parentKey, objKey)
		ops = append(ops, clientv3.OpDelete(dbKey))
	}
	return ops
}

// GetChildrenKeys gets the current reversed owner references for a parent key
func (b *OwnerReferenceOpBuilder) GetChildrenKeys(ctx context.Context, parentKey sdkmeta.ObjectKey) ([]sdkmeta.ObjectKey, error) {
	parentPrefixDbKey := buildOwnerReferenceIndexDbKeyPrefix(parentKey)

	kvs, err := b.clientWrapper.List(ctx, parentPrefixDbKey, -1)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get owner references from etcd")
	}

	childKeys := make([]sdkmeta.ObjectKey, len(kvs))
	for _, kv := range kvs {
		_, childKey, err := parseOwnerReferenceIndexDbKey(kv.Key)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse index db key")
		}
		childKeys = append(childKeys, childKey)
	}

	return childKeys, nil
}

// QueryChildren queries all child resources for a given parent key
func (b *OwnerReferenceOpBuilder) QueryChildren(ctx context.Context, parentKey sdkmeta.ObjectKey) ([]*sdkmeta.Object, error) {
	childKeys, err := b.GetChildrenKeys(ctx, parentKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get reversed owner references")
	}

	var children []*sdkmeta.Object

	for _, childKey := range childKeys {
		child, err := b.store.Get(ctx, childKey)
		if err != nil {
			if IsNotFoundError(err) {
				b.logger.Warn("child resource not found by reversed reference",
					zap.String("childKey", objectKeyToDbKey(childKey)),
					zap.String("parentKey", objectKeyToDbKey(parentKey)))
				continue
			}
			return nil, errors.Wrap(err, "failed to get child resource")
		}

		children = append(children, child)
	}

	return children, nil
}

func buildOwnerReferenceIndexDbKeyPrefix(parentKey sdkmeta.ObjectKey) string {
	return fmt.Sprintf("/index/owner-reference/%s", objectKeyToDbKey(parentKey))
}

func buildOwnerReferenceIndexDbKey(parentKey sdkmeta.ObjectKey, childKey sdkmeta.ObjectKey) string {
	return fmt.Sprintf("/index/owner-reference/%s/%s", objectKeyToDbKey(parentKey), objectKeyToDbKey(childKey))
}

func parseOwnerReferenceIndexDbKey(dbKey string) (sdkmeta.ObjectKey, sdkmeta.ObjectKey, error) {
	dbKey = strings.TrimPrefix(dbKey, "/index/owner-reference/")
	parts := strings.Split(dbKey, "/")
	parentKey, err := parseObjectKey(parts[1])
	if err != nil {
		return sdkmeta.ObjectKey{}, sdkmeta.ObjectKey{}, err
	}
	childKey, err := parseObjectKey(parts[0])
	if err != nil {
		return sdkmeta.ObjectKey{}, sdkmeta.ObjectKey{}, err
	}
	return parentKey, childKey, nil
}

func CalculateOwnerReferenceDiff(oldOwnerRefs, newOwnerRefs []sdkmeta.OwnerReference) ([]sdkmeta.OwnerReference, []sdkmeta.OwnerReference) {
	if oldOwnerRefs == nil {
		oldOwnerRefs = make([]sdkmeta.OwnerReference, 0)
	}
	if newOwnerRefs == nil {
		newOwnerRefs = make([]sdkmeta.OwnerReference, 0)
	}

	oldMap := make(map[string]sdkmeta.OwnerReference)
	for _, ref := range oldOwnerRefs {
		key := ref.TypeMeta.Kind + "/" + ref.Name
		oldMap[key] = ref
	}

	newMap := make(map[string]sdkmeta.OwnerReference)
	for _, ref := range newOwnerRefs {
		key := ref.TypeMeta.Kind + "/" + ref.Name
		newMap[key] = ref
	}

	var deleted []sdkmeta.OwnerReference
	for key, ref := range oldMap {
		if _, exists := newMap[key]; !exists {
			deleted = append(deleted, ref)
		}
	}

	var created []sdkmeta.OwnerReference
	for key, ref := range newMap {
		if _, exists := oldMap[key]; !exists {
			created = append(created, ref)
		}
	}

	return deleted, created
}
