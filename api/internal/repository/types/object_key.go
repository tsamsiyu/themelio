package types

import (
	"fmt"
	"strings"

	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ObjectKey represents a unique identifier for a Kubernetes resource
type ObjectKey struct {
	Group     string
	Version   string
	Kind      string
	Namespace string
	Name      string
}

// NewObjectKey creates a new ObjectKey
func NewObjectKey(group, version, kind, namespace, name string) ObjectKey {
	if namespace == "" {
		namespace = "default"
	}
	return ObjectKey{
		Group:     group,
		Version:   version,
		Kind:      kind,
		Namespace: namespace,
		Name:      name,
	}
}

// NewObjectKeyFromResource creates a new ObjectKey from an unstructured resource
func NewObjectKeyFromResource(resource *unstructured.Unstructured) ObjectKey {
	namespace := resource.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}

	apiVersion := resource.GetAPIVersion()
	group := ""
	version := apiVersion
	if strings.Contains(apiVersion, "/") {
		parts := strings.Split(apiVersion, "/")
		group = parts[0]
		version = parts[1]
	}

	return ObjectKey{
		Group:     group,
		Version:   version,
		Kind:      resource.GetKind(),
		Namespace: namespace,
		Name:      resource.GetName(),
	}
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

// ParseObjectKey parses a key string back to ObjectKey (requires all 5 parts)
func ParseObjectKey(key string) (ObjectKey, error) {
	key = strings.TrimPrefix(key, "/")
	parts := strings.Split(key, "/")

	if len(parts) != 5 {
		return ObjectKey{}, fmt.Errorf("invalid object key format: expected exactly 5 parts, got %d", len(parts))
	}

	return ObjectKey{
		Group:     parts[0],
		Version:   parts[1],
		Kind:      parts[2],
		Namespace: parts[3],
		Name:      parts[4],
	}, nil
}

// HasPrefix checks if the given key is a prefix of this ObjectKey
func (k ObjectKey) HasPrefix(prefix string) bool {
	keyStr := k.String()
	return strings.HasPrefix(keyStr, prefix)
}

// String returns the string representation of the ObjectKey
func (k ObjectKey) String() string {
	return fmt.Sprintf("/%s/%s/%s/%s/%s", k.Group, k.Version, k.Kind, k.Namespace, k.Name)
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
