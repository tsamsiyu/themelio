package types

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ReversedOwnerReferenceSet map[string]struct{}

type OwnerReferenceDiff struct {
	Deleted []metav1.OwnerReference
	Created []metav1.OwnerReference
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

func OwnerRefToObjectKey(ownerRef metav1.OwnerReference, namespace string) ObjectKey {
	group := ""
	version := ownerRef.APIVersion

	if strings.Contains(ownerRef.APIVersion, "/") {
		parts := strings.Split(ownerRef.APIVersion, "/")
		group = parts[0]
		version = parts[1]
	}

	return NewNamespacedObjectKey(group, version, ownerRef.Kind, namespace, ownerRef.Name)
}

func CalculateOwnerReferenceDiff(oldOwnerRefs, newOwnerRefs []metav1.OwnerReference) OwnerReferenceDiff {
	oldMap := make(map[string]metav1.OwnerReference)
	for _, ref := range oldOwnerRefs {
		key := ref.Kind + "/" + ref.Name
		oldMap[key] = ref
	}

	newMap := make(map[string]metav1.OwnerReference)
	for _, ref := range newOwnerRefs {
		key := ref.Kind + "/" + ref.Name
		newMap[key] = ref
	}

	var deleted []metav1.OwnerReference
	for key, ref := range oldMap {
		if _, exists := newMap[key]; !exists {
			deleted = append(deleted, ref)
		}
	}

	var created []metav1.OwnerReference
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
