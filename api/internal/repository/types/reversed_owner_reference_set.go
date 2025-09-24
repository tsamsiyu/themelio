package types

import (
	"strings"

	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type ReversedOwnerReferenceSet map[string]struct{}

type OwnerReferenceDiff struct {
	Deleted []sdkmeta.OwnerReference
	Created []sdkmeta.OwnerReference
}

func NewReversedOwnerReferenceSet() ReversedOwnerReferenceSet {
	return make(ReversedOwnerReferenceSet)
}

func (s ReversedOwnerReferenceSet) Put(key string) {
	s[key] = struct{}{}
}

func (s ReversedOwnerReferenceSet) Delete(key string) {
	delete(s, key)
}

func (s ReversedOwnerReferenceSet) Encode() string {
	var keys []string
	for key := range s {
		keys = append(keys, key)
	}
	return strings.Join(keys, ",")
}

func (s ReversedOwnerReferenceSet) Decode(data string) {
	if data == "" {
		return
	}
	keys := strings.Split(data, ",")
	for _, key := range keys {
		if key != "" {
			s[key] = struct{}{}
		}
	}
}

func CalculateOwnerReferenceDiffFromObjects(oldResource, newResource *sdkmeta.Object) OwnerReferenceDiff {
	var oldOwnerRefs, newOwnerRefs []sdkmeta.OwnerReference

	if oldResource != nil && oldResource.ObjectMeta != nil {
		oldOwnerRefs = oldResource.ObjectMeta.OwnerReferences
	}

	if newResource != nil && newResource.ObjectMeta != nil {
		newOwnerRefs = newResource.ObjectMeta.OwnerReferences
	}

	return CalculateOwnerReferenceDiff(oldOwnerRefs, newOwnerRefs)
}

func CalculateOwnerReferenceDiff(oldOwnerRefs, newOwnerRefs []sdkmeta.OwnerReference) OwnerReferenceDiff {
	if oldOwnerRefs == nil {
		oldOwnerRefs = []sdkmeta.OwnerReference{}
	}
	if newOwnerRefs == nil {
		newOwnerRefs = []sdkmeta.OwnerReference{}
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

	return OwnerReferenceDiff{
		Deleted: deleted,
		Created: created,
	}
}
