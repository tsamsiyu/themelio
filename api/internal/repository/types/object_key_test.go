package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestNewClusterObjectKey(t *testing.T) {
	key := NewClusterObjectKey("rbac.authorization.k8s.io", "v1", "ClusterRole", "admin")

	assert.Equal(t, "rbac.authorization.k8s.io", key.Group)
	assert.Equal(t, "v1", key.Version)
	assert.Equal(t, "ClusterRole", key.Kind)
	assert.Equal(t, "", key.Namespace)
	assert.Equal(t, "admin", key.Name)
	assert.Equal(t, "/rbac.authorization.k8s.io/v1/ClusterRole/admin", key.ToKey())
}

func TestNewNamespacedObjectKey(t *testing.T) {
	key := NewNamespacedObjectKey("rbac.authorization.k8s.io", "v1", "Role", "default", "admin")

	assert.Equal(t, "rbac.authorization.k8s.io", key.Group)
	assert.Equal(t, "v1", key.Version)
	assert.Equal(t, "Role", key.Kind)
	assert.Equal(t, "default", key.Namespace)
	assert.Equal(t, "admin", key.Name)
	assert.Equal(t, "/rbac.authorization.k8s.io/v1/Role/default/admin", key.ToKey())
}

func TestNewObjectKeyFromResource_ClusterScoped(t *testing.T) {
	resource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "rbac.authorization.k8s.io/v1",
			"kind":       "ClusterRole",
			"metadata": map[string]interface{}{
				"name": "admin",
			},
		},
	}

	key := NewObjectKeyFromResource(resource)

	assert.Equal(t, "rbac.authorization.k8s.io", key.Group)
	assert.Equal(t, "v1", key.Version)
	assert.Equal(t, "ClusterRole", key.Kind)
	assert.Equal(t, "", key.Namespace)
	assert.Equal(t, "admin", key.Name)
	assert.Equal(t, "/rbac.authorization.k8s.io/v1/ClusterRole/admin", key.ToKey())
}

func TestNewObjectKeyFromResource_Namespaced(t *testing.T) {
	resource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "rbac.authorization.k8s.io/v1",
			"kind":       "Role",
			"metadata": map[string]interface{}{
				"name":      "admin",
				"namespace": "default",
			},
		},
	}

	key := NewObjectKeyFromResource(resource)

	assert.Equal(t, "rbac.authorization.k8s.io", key.Group)
	assert.Equal(t, "v1", key.Version)
	assert.Equal(t, "Role", key.Kind)
	assert.Equal(t, "default", key.Namespace)
	assert.Equal(t, "admin", key.Name)
	assert.Equal(t, "/rbac.authorization.k8s.io/v1/Role/default/admin", key.ToKey())
}

func TestParseObjectKey_ClusterScoped(t *testing.T) {
	keyStr := "/rbac.authorization.k8s.io/v1/ClusterRole/admin"
	key, err := ParseObjectKey(keyStr)

	assert.NoError(t, err)
	assert.Equal(t, "rbac.authorization.k8s.io", key.Group)
	assert.Equal(t, "v1", key.Version)
	assert.Equal(t, "ClusterRole", key.Kind)
	assert.Equal(t, "", key.Namespace)
	assert.Equal(t, "admin", key.Name)
}

func TestParseObjectKey_Namespaced(t *testing.T) {
	keyStr := "/rbac.authorization.k8s.io/v1/Role/default/admin"
	key, err := ParseObjectKey(keyStr)

	assert.NoError(t, err)
	assert.Equal(t, "rbac.authorization.k8s.io", key.Group)
	assert.Equal(t, "v1", key.Version)
	assert.Equal(t, "Role", key.Kind)
	assert.Equal(t, "default", key.Namespace)
	assert.Equal(t, "admin", key.Name)
}

func TestParseObjectKey_InvalidFormat(t *testing.T) {
	keyStr := "/rbac.authorization.k8s.io/v1/ClusterRole"
	_, err := ParseObjectKey(keyStr)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 4 or 5 parts, got 3")
}

func TestObjectKey_ToResourceKey(t *testing.T) {
	clusterKey := NewClusterObjectKey("rbac.authorization.k8s.io", "v1", "ClusterRole", "admin")
	resourceKey := clusterKey.ToResourceKey()

	assert.Equal(t, "rbac.authorization.k8s.io", resourceKey.Group)
	assert.Equal(t, "v1", resourceKey.Version)
	assert.Equal(t, "ClusterRole", resourceKey.Kind)
	assert.Equal(t, "", resourceKey.Namespace)
	assert.Equal(t, "/rbac.authorization.k8s.io/v1/ClusterRole", resourceKey.ToKey())

	namespacedKey := NewNamespacedObjectKey("rbac.authorization.k8s.io", "v1", "Role", "default", "admin")
	resourceKey2 := namespacedKey.ToResourceKey()

	assert.Equal(t, "rbac.authorization.k8s.io", resourceKey2.Group)
	assert.Equal(t, "v1", resourceKey2.Version)
	assert.Equal(t, "Role", resourceKey2.Kind)
	assert.Equal(t, "default", resourceKey2.Namespace)
	assert.Equal(t, "/rbac.authorization.k8s.io/v1/Role/default", resourceKey2.ToKey())
}
