package repository

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

// NewObjectKey creates a new ObjectKey from group, version, kind, namespace, and name
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

// IsFull returns true if the ObjectKey has a name (for full resource identification)
func (k ObjectKey) IsFull() bool {
	return k.Name != ""
}

func (k ObjectKey) ToKeyWithoutName() string {
	return fmt.Sprintf("/%s/%s/%s/%s", k.Group, k.Version, k.Kind, k.Namespace)
}

// ToKey creates a key from the ObjectKey (with leading "/" for etcd compatibility)
func (k ObjectKey) ToKey() string {
	if k.Name != "" {
		return fmt.Sprintf("/%s/%s/%s/%s/%s", k.Group, k.Version, k.Kind, k.Namespace, k.Name)
	}
	return fmt.Sprintf("/%s/%s/%s/%s", k.Group, k.Version, k.Kind, k.Namespace)
}

// ParseKey parses a key string back to ObjectKey
func ParseKey(key string) (ObjectKey, error) {
	key = strings.TrimPrefix(key, "/")

	parts := strings.Split(key, "/")
	if len(parts) < 4 {
		return ObjectKey{}, fmt.Errorf("invalid key format: %s", key)
	}

	objKey := ObjectKey{
		Group:     parts[0],
		Version:   parts[1],
		Kind:      parts[2],
		Namespace: parts[3],
	}

	// If there's a 5th part, it's the name (full resource key)
	if len(parts) >= 5 {
		objKey.Name = parts[4]
	}

	return objKey, nil
}

// HasPrefix checks if the given watch key is a prefix of this ObjectKey
func (k ObjectKey) HasPrefix(watchKey string) bool {
	return strings.HasPrefix(k.ToKey(), watchKey)
}

// String returns a string representation of the ObjectKey
func (k ObjectKey) String() string {
	return k.ToKey()
}

// MarshalLogObject implements zapcore.ObjectMarshaler interface
func (k ObjectKey) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("group", k.Group)
	enc.AddString("version", k.Version)
	enc.AddString("kind", k.Kind)
	enc.AddString("namespace", k.Namespace)
	enc.AddString("name", k.Name)
	return nil
}
