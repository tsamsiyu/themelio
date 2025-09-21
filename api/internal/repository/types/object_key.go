package types

import (
	"fmt"
	"strings"

	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ObjectKey represents a unique identifier for a Kubernetes resource
type ObjectKey struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

func NewClusterObjectKey(group, version, kind, name string) ObjectKey {
	return ObjectKey{
		Group:     group,
		Version:   version,
		Kind:      kind,
		Namespace: "",
		Name:      name,
	}
}

func NewNamespacedObjectKey(group, version, kind, namespace, name string) ObjectKey {
	return ObjectKey{
		Group:     group,
		Version:   version,
		Kind:      kind,
		Namespace: namespace,
		Name:      name,
	}
}

func NewObjectKeyFromResource(resource *unstructured.Unstructured) ObjectKey {
	apiVersion := resource.GetAPIVersion()
	group := ""
	version := apiVersion
	if strings.Contains(apiVersion, "/") {
		parts := strings.Split(apiVersion, "/")
		group = parts[0]
		version = parts[1]
	}

	kind := resource.GetKind()
	namespace := resource.GetNamespace()
	name := resource.GetName()

	if namespace == "" {
		return NewClusterObjectKey(group, version, kind, name)
	}
	return NewNamespacedObjectKey(group, version, kind, namespace, name)
}

// ToGroupVersionKind returns the GroupVersionKind part of the ObjectKey
func (k ObjectKey) ToGroupVersionKind() GroupVersionKind {
	return GroupVersionKind{
		Group:   k.Group,
		Version: k.Version,
		Kind:    k.Kind,
	}
}

// ToResourceKey returns the ResourceKey part of the ObjectKey (without name)
func (k ObjectKey) ToResourceKey() ResourceKey {
	return ResourceKey{
		Group:     k.Group,
		Version:   k.Version,
		Kind:      k.Kind,
		Namespace: k.Namespace,
	}
}

func ParseObjectKey(key string) (ObjectKey, error) {
	key = strings.TrimPrefix(key, "/")
	parts := strings.Split(key, "/")

	if len(parts) == 4 {
		return ObjectKey{
			Group:     parts[0],
			Version:   parts[1],
			Kind:      parts[2],
			Namespace: "",
			Name:      parts[3],
		}, nil
	} else if len(parts) == 5 {
		return ObjectKey{
			Group:     parts[0],
			Version:   parts[1],
			Kind:      parts[2],
			Namespace: parts[3],
			Name:      parts[4],
		}, nil
	}
	return ObjectKey{}, fmt.Errorf("invalid object key format: expected 4 or 5 parts, got %d", len(parts))
}

// HasPrefix checks if the given key is a prefix of this ObjectKey
func (k ObjectKey) HasPrefix(prefix string) bool {
	keyStr := k.String()
	return strings.HasPrefix(keyStr, prefix)
}

func (k ObjectKey) ToKey() string {
	if k.Namespace == "" {
		return fmt.Sprintf("/%s/%s/%s/%s", k.Group, k.Version, k.Kind, k.Name)
	}
	return fmt.Sprintf("/%s/%s/%s/%s/%s", k.Group, k.Version, k.Kind, k.Namespace, k.Name)
}

// String returns the string representation of the ObjectKey
func (k ObjectKey) String() string {
	return k.ToKey()
}

// MarshalLogObject implements zapcore.ObjectMarshaler for structured logging
func (k ObjectKey) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("group", k.Group)
	enc.AddString("version", k.Version)
	enc.AddString("kind", k.Kind)
	enc.AddString("namespace", k.Namespace)
	enc.AddString("name", k.Name)
	return nil
}
