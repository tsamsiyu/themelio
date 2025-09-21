package types

import (
	"fmt"
	"strings"

	"go.uber.org/zap/zapcore"
)

// ResourceKey represents a resource type and namespace (without specific name)
type ResourceKey struct {
	Group     string
	Version   string
	Kind      string
	Namespace string
}

func NewClusterResourceKey(group, version, kind string) ResourceKey {
	return ResourceKey{
		Group:     group,
		Version:   version,
		Kind:      kind,
		Namespace: "",
	}
}

func NewNamespacedResourceKey(group, version, kind, namespace string) ResourceKey {
	return ResourceKey{
		Group:     group,
		Version:   version,
		Kind:      kind,
		Namespace: namespace,
	}
}

// NewResourceKeyFromObjectKey creates a ResourceKey from an ObjectKey (without name)
func NewResourceKeyFromObjectKey(objectKey ObjectKey) ResourceKey {
	return ResourceKey{
		Group:     objectKey.Group,
		Version:   objectKey.Version,
		Kind:      objectKey.Kind,
		Namespace: objectKey.Namespace,
	}
}

// ToGroupVersionKind returns the GroupVersionKind part of the ResourceKey
func (k ResourceKey) ToGroupVersionKind() GroupVersionKind {
	return GroupVersionKind{
		Group:   k.Group,
		Version: k.Version,
		Kind:    k.Kind,
	}
}

func (k ResourceKey) ToKey() string {
	if k.Namespace == "" {
		return fmt.Sprintf("/%s/%s/%s", k.Group, k.Version, k.Kind)
	}
	return fmt.Sprintf("/%s/%s/%s/%s", k.Group, k.Version, k.Kind, k.Namespace)
}

// String returns the string representation of the ResourceKey
func (k ResourceKey) String() string {
	return k.ToKey()
}

func ParseResourceKey(key string) (ResourceKey, error) {
	key = strings.TrimPrefix(key, "/")
	parts := strings.Split(key, "/")

	if len(parts) == 3 {
		return ResourceKey{
			Group:     parts[0],
			Version:   parts[1],
			Kind:      parts[2],
			Namespace: "",
		}, nil
	} else if len(parts) == 4 {
		return ResourceKey{
			Group:     parts[0],
			Version:   parts[1],
			Kind:      parts[2],
			Namespace: parts[3],
		}, nil
	}
	return ResourceKey{}, fmt.Errorf("invalid resource key format: expected 3 or 4 parts, got %d", len(parts))
}

// MarshalLogObject implements zapcore.ObjectMarshaler for structured logging
func (k ResourceKey) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("group", k.Group)
	enc.AddString("version", k.Version)
	enc.AddString("kind", k.Kind)
	enc.AddString("namespace", k.Namespace)
	return nil
}
