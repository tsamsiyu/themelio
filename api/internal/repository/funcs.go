package repository

import (
	"fmt"
	"strings"

	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

func objectKeyToDbKey(key sdkmeta.ObjectKey) string {
	if key.Group == "" || key.Version == "" || key.Kind == "" || key.Name == "" {
		return ""
	}

	if key.Namespace == "" {
		return fmt.Sprintf("/%s/%s/%s/%s", key.Group, key.Version, key.Kind, key.Name)
	}
	return fmt.Sprintf("/%s/%s/%s/%s/%s", key.Group, key.Version, key.Kind, key.Namespace, key.Name)
}

func objectTypeToDbKey(objType *sdkmeta.ObjectType) string {
	if objType.Namespace == "" {
		return fmt.Sprintf("/%s/%s/%s", objType.Group, objType.Version, objType.Kind)
	}
	return fmt.Sprintf("/%s/%s/%s/%s", objType.Group, objType.Version, objType.Kind, objType.Namespace)
}

func schemaDbKey(group, kind string) string {
	return fmt.Sprintf("/schema/%s/%s", group, kind)
}

func deletionDbKey(key sdkmeta.ObjectKey) string {
	return fmt.Sprintf("/deletion%s", objectKeyToDbKey(key))
}

func deletionLockDbKey(lockKey string) string {
	return fmt.Sprintf("/deletion-lock/%s", lockKey)
}

func parseObjectKey(key string) (sdkmeta.ObjectKey, error) {
	key = strings.TrimPrefix(key, "/")
	parts := strings.Split(key, "/")

	if len(parts) == 4 {
		return sdkmeta.ObjectKey{
			ObjectType: sdkmeta.ObjectType{
				Group:     parts[0],
				Version:   parts[1],
				Kind:      parts[2],
				Namespace: "",
			},
			Name: parts[3],
		}, nil
	} else if len(parts) == 5 {
		return sdkmeta.ObjectKey{
			ObjectType: sdkmeta.ObjectType{
				Group:     parts[0],
				Version:   parts[1],
				Kind:      parts[2],
				Namespace: parts[3],
			},
			Name: parts[4],
		}, nil
	}
	return sdkmeta.ObjectKey{}, fmt.Errorf("invalid object key format: expected 4 or 5 parts, got %d", len(parts))
}

func OwnerRefToObjectKey(ownerRef sdkmeta.OwnerReference, namespace string) sdkmeta.ObjectKey {
	return sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     ownerRef.TypeMeta.Group,
			Version:   ownerRef.TypeMeta.Version,
			Kind:      ownerRef.TypeMeta.Kind,
			Namespace: namespace,
		},
		Name: ownerRef.Name,
	}
}
