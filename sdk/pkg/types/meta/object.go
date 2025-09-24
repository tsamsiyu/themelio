package meta

import "time"

type ObjectType struct {
	Group     string `json:"group" validate:"required"`
	Version   string `json:"version" validate:"required"`
	Kind      string `json:"kind" validate:"required"`
	Namespace string `json:"namespace"`
}

type ObjectKey struct {
	ObjectType `json:",inline" validate:"required"`
	Name       string `json:"name" validate:"required"`
}

type ObjectMeta struct {
	Labels          map[string]string `json:"labels"`
	Annotations     map[string]string `json:"annotations"`
	OwnerReferences []OwnerReference  `json:"ownerReferences"`
	Finalizers      []string          `json:"finalizers"`
}

type SystemMeta struct {
	UID               string     `json:"uid"`
	Revision          string     `json:"revision"`
	CreationTimestamp *time.Time `json:"creationTimestamp"`
	DeletionTimestamp *time.Time `json:"deletionTimestamp"`
}

type OwnerReference struct {
	TypeMeta           *ObjectType `json:"typeMeta" validate:"required"`
	Name               string      `json:"name" validate:"required"`
	UID                string      `json:"uid" validate:"required"`
	BlockOwnerDeletion bool        `json:"blockOwnerDeletion"`
}

func (or OwnerReference) ToObjectKey() ObjectKey {
	return ObjectKey{
		ObjectType: ObjectType{
			Group:     or.TypeMeta.Group,
			Version:   or.TypeMeta.Version,
			Kind:      or.TypeMeta.Kind,
			Namespace: or.TypeMeta.Namespace,
		},
		Name: or.Name,
	}
}

type Object struct {
	ObjectKey  *ObjectKey  `json:"key" validate:"required"`
	ObjectMeta *ObjectMeta `json:"meta" validate:"required"`
	SystemMeta *SystemMeta `json:"system"`
	Spec       interface{} `json:"spec"`
	Status     interface{} `json:"status"`
}
