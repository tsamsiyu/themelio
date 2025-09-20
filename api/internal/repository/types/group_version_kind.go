package types

import (
	"fmt"

	"go.uber.org/zap/zapcore"
)

type GroupVersionKind struct {
	Group   string
	Version string
	Kind    string
}

func NewGroupVersionKind(group, version, kind string) GroupVersionKind {
	return GroupVersionKind{
		Group:   group,
		Version: version,
		Kind:    kind,
	}
}

// ToKey returns the string representation of the GroupVersionKind for database operations
func (gvk GroupVersionKind) ToKey() string {
	return fmt.Sprintf("%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind)
}

func (gvk GroupVersionKind) String() string {
	return gvk.ToKey()
}

func (gvk GroupVersionKind) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("group", gvk.Group)
	enc.AddString("version", gvk.Version)
	enc.AddString("kind", gvk.Kind)
	return nil
}
