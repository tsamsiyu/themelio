package repository_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	"github.com/tsamsiyu/themelio/api/mocks"
)

func TestResourceRepository_Replace_NewResource(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := types.NewNamespacedObjectKey("example.com", "v1", "TestResource", "default", "new-resource")
	resource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "TestResource",
			"metadata": map[string]interface{}{
				"name":      "new-resource",
				"namespace": "default",
				"uid":       "new-uid",
			},
			"spec": map[string]interface{}{
				"replicas": 3,
			},
		},
	}

	// Mock expectations for new resource creation
	// 1. Get existing resource - should return not found
	mockStore.EXPECT().Get(ctx, key).Return(nil, types.NewNotFoundError("resource not found"))

	// 2. Build put operation for the new resource
	mockStore.EXPECT().BuildPutTxOp(key, resource).Return(clientv3.OpPut(key.String(), "{}"), nil)

	// 3. Execute transaction - since no existing resource, no owner reference operations needed
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test - should successfully create new resource
	err := repo.Replace(ctx, key, resource)
	assert.NoError(t, err)
}

func TestResourceRepository_Replace_NewResource_WithOwnerReferences(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := types.NewNamespacedObjectKey("example.com", "v1", "TestResource", "default", "new-resource")
	resource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "TestResource",
			"metadata": map[string]interface{}{
				"name":      "new-resource",
				"namespace": "default",
				"uid":       "new-uid",
				"ownerReferences": []interface{}{
					map[string]interface{}{
						"apiVersion": "apps/v1",
						"kind":       "Deployment",
						"name":       "parent-deployment",
						"uid":        "parent-uid",
					},
				},
			},
			"spec": map[string]interface{}{
				"replicas": 3,
			},
		},
	}

	// Mock expectations for new resource creation with owner references
	// 1. Get existing resource - should return not found
	mockStore.EXPECT().Get(ctx, key).Return(nil, types.NewNotFoundError("resource not found"))

	// 2. Build put operation for the new resource
	mockStore.EXPECT().BuildPutTxOp(key, resource).Return(clientv3.OpPut(key.String(), "{}"), nil)

	// 3. BuildDiffOperations will be called to handle owner reference creation
	// Since this is a new resource, it will create owner references
	// Mock the GetReversedOwnerReferences call that happens during BuildAddOperations
	parentKey := types.NewNamespacedObjectKey("apps", "v1", "Deployment", "default", "parent-deployment")
	refKey := "/ref" + parentKey.String()
	mockClient.EXPECT().Get(ctx, refKey).Return([]byte(""), nil)

	// 4. Execute transaction with owner reference operations
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test - should successfully create new resource with owner references
	err := repo.Replace(ctx, key, resource)
	assert.NoError(t, err)
}

func TestResourceRepository_Replace_UpdateExistingResource(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := types.NewNamespacedObjectKey("example.com", "v1", "TestResource", "default", "test-resource")

	// Existing resource with owner references
	existingResource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "TestResource",
			"metadata": map[string]interface{}{
				"name":      "test-resource",
				"namespace": "default",
				"ownerReferences": []interface{}{
					map[string]interface{}{
						"apiVersion": "apps/v1",
						"kind":       "Deployment",
						"name":       "parent-deployment",
						"uid":        "parent-uid",
					},
				},
			},
			"spec": map[string]interface{}{
				"replicas": 1,
			},
		},
	}

	// New resource with different owner references
	newResource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "TestResource",
			"metadata": map[string]interface{}{
				"name":      "test-resource",
				"namespace": "default",
				"ownerReferences": []interface{}{
					map[string]interface{}{
						"apiVersion": "apps/v1",
						"kind":       "StatefulSet",
						"name":       "new-parent",
						"uid":        "new-parent-uid",
					},
				},
			},
			"spec": map[string]interface{}{
				"replicas": 3,
			},
		},
	}

	// Mock expectations for updating existing resource with owner reference changes
	mockStore.EXPECT().Get(ctx, key).Return(existingResource, nil)
	mockStore.EXPECT().BuildPutTxOp(key, newResource).Return(clientv3.OpPut(key.String(), "{}"), nil)

	// Mock owner reference operations - these are complex internal calls
	mockClient.EXPECT().Get(ctx, mock.Anything).Return([]byte("{}"), nil).Maybe()
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test
	err := repo.Replace(ctx, key, newResource)
	assert.NoError(t, err)
}

func TestResourceRepository_Replace_NoOwnerReferenceChanges(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := types.NewNamespacedObjectKey("example.com", "v1", "TestResource", "default", "test-resource")

	// Existing resource
	existingResource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "TestResource",
			"metadata": map[string]interface{}{
				"name":      "test-resource",
				"namespace": "default",
				"ownerReferences": []interface{}{
					map[string]interface{}{
						"apiVersion": "apps/v1",
						"kind":       "Deployment",
						"name":       "parent-deployment",
						"uid":        "parent-uid",
					},
				},
			},
			"spec": map[string]interface{}{
				"replicas": 1,
			},
		},
	}

	// New resource with same owner references
	newResource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "TestResource",
			"metadata": map[string]interface{}{
				"name":      "test-resource",
				"namespace": "default",
				"ownerReferences": []interface{}{
					map[string]interface{}{
						"apiVersion": "apps/v1",
						"kind":       "Deployment",
						"name":       "parent-deployment",
						"uid":        "parent-uid",
					},
				},
			},
			"spec": map[string]interface{}{
				"replicas": 3, // Only spec changed, owner refs same
			},
		},
	}

	// Mock expectations - no owner reference operations needed
	mockStore.EXPECT().Get(ctx, key).Return(existingResource, nil)
	mockStore.EXPECT().BuildPutTxOp(key, newResource).Return(clientv3.OpPut(key.String(), "{}"), nil)
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test
	err := repo.Replace(ctx, key, newResource)
	assert.NoError(t, err)
}
