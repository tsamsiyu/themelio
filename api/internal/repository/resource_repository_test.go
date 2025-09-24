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

func TestResourceRepository_Get(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := types.NewNamespacedObjectKey("example.com", "v1", "TestResource", "default", "test-resource")
	expectedResource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "TestResource",
			"metadata": map[string]interface{}{
				"name":      "test-resource",
				"namespace": "default",
			},
		},
	}

	// Mock expectations
	mockStore.EXPECT().Get(ctx, key).Return(expectedResource, nil)

	// Test
	result, err := repo.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, expectedResource, result)
}

func TestResourceRepository_List(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := types.NewNamespacedResourceKey("example.com", "v1", "TestResource", "default")
	expectedResources := []*unstructured.Unstructured{
		{
			Object: map[string]interface{}{
				"apiVersion": "example.com/v1",
				"kind":       "TestResource",
				"metadata": map[string]interface{}{
					"name":      "test-resource-1",
					"namespace": "default",
				},
			},
		},
	}

	// Mock expectations
	mockStore.EXPECT().List(ctx, key, 100).Return(expectedResources, nil)

	// Test
	result, err := repo.List(ctx, key, 100)
	assert.NoError(t, err)
	assert.Equal(t, expectedResources, result)
}

func TestResourceRepository_MarkDeleted(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := types.NewNamespacedObjectKey("example.com", "v1", "TestResource", "default", "test-resource")
	resource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "TestResource",
			"metadata": map[string]interface{}{
				"name":      "test-resource",
				"namespace": "default",
			},
		},
	}

	// Mock expectations - simplified to match actual behavior
	mockStore.EXPECT().Get(ctx, key).Return(resource, nil)
	mockStore.EXPECT().BuildPutTxOp(key, mock.Anything).Return(clientv3.OpPut(key.String(), "{}"), nil)
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test
	err := repo.MarkDeleted(ctx, key)
	assert.NoError(t, err)
}
