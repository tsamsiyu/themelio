package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewClusterResourceKey(t *testing.T) {
	key := NewClusterResourceKey("rbac.authorization.k8s.io", "v1", "ClusterRole")

	assert.Equal(t, "rbac.authorization.k8s.io", key.Group)
	assert.Equal(t, "v1", key.Version)
	assert.Equal(t, "ClusterRole", key.Kind)
	assert.Equal(t, "", key.Namespace)
	assert.Equal(t, "/rbac.authorization.k8s.io/v1/ClusterRole", key.ToKey())
}

func TestNewNamespacedResourceKey(t *testing.T) {
	key := NewNamespacedResourceKey("rbac.authorization.k8s.io", "v1", "Role", "default")

	assert.Equal(t, "rbac.authorization.k8s.io", key.Group)
	assert.Equal(t, "v1", key.Version)
	assert.Equal(t, "Role", key.Kind)
	assert.Equal(t, "default", key.Namespace)
	assert.Equal(t, "/rbac.authorization.k8s.io/v1/Role/default", key.ToKey())
}

func TestParseResourceKey_ClusterScoped(t *testing.T) {
	keyStr := "/rbac.authorization.k8s.io/v1/ClusterRole"
	key, err := ParseResourceKey(keyStr)

	assert.NoError(t, err)
	assert.Equal(t, "rbac.authorization.k8s.io", key.Group)
	assert.Equal(t, "v1", key.Version)
	assert.Equal(t, "ClusterRole", key.Kind)
	assert.Equal(t, "", key.Namespace)
}

func TestParseResourceKey_Namespaced(t *testing.T) {
	keyStr := "/rbac.authorization.k8s.io/v1/Role/default"
	key, err := ParseResourceKey(keyStr)

	assert.NoError(t, err)
	assert.Equal(t, "rbac.authorization.k8s.io", key.Group)
	assert.Equal(t, "v1", key.Version)
	assert.Equal(t, "Role", key.Kind)
	assert.Equal(t, "default", key.Namespace)
}

func TestParseResourceKey_InvalidFormat(t *testing.T) {
	keyStr := "/rbac.authorization.k8s.io/v1"
	_, err := ParseResourceKey(keyStr)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 3 or 4 parts, got 2")
}

func TestResourceKey_ToGroupVersionKind(t *testing.T) {
	clusterKey := NewClusterResourceKey("rbac.authorization.k8s.io", "v1", "ClusterRole")
	gvk := clusterKey.ToGroupVersionKind()

	assert.Equal(t, "rbac.authorization.k8s.io", gvk.Group)
	assert.Equal(t, "v1", gvk.Version)
	assert.Equal(t, "ClusterRole", gvk.Kind)

	namespacedKey := NewNamespacedResourceKey("rbac.authorization.k8s.io", "v1", "Role", "default")
	gvk2 := namespacedKey.ToGroupVersionKind()

	assert.Equal(t, "rbac.authorization.k8s.io", gvk2.Group)
	assert.Equal(t, "v1", gvk2.Version)
	assert.Equal(t, "Role", gvk2.Kind)
}
