package repository_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	"github.com/tsamsiyu/themelio/api/mocks"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

func TestResourceRepository_Delete_ResourceWithoutOwnerReferences(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}
	resource := &sdkmeta.Object{
		ObjectKey: &key,
		ObjectMeta: &sdkmeta.ObjectMeta{
			Labels:          map[string]string{},
			Annotations:     map[string]string{},
			OwnerReferences: []sdkmeta.OwnerReference{}, // No owner references
		},
		SystemMeta: &sdkmeta.SystemMeta{
			UID: "test-uid",
		},
		Spec: map[string]interface{}{
			"replicas": 3,
		},
	}

	// Mock expectations for resource without owner references
	// 1. Get the resource to delete
	mockStore.EXPECT().Get(ctx, key).Return(resource, nil)

	// 2. BuildDropOperations will be called with empty owner references
	// Since there are no owner references, BuildDropOperations will return empty operations
	// 3. QueryChildResources will be called to find child resources
	// QueryChildResources calls GetReversedOwnerReferences which calls client.Get
	// Since there are no owner references, it will return empty data
	refKey := "/ref/example.com/v1/TestResource/default/test-resource"
	mockClient.EXPECT().Get(ctx, refKey).Return([]byte(""), nil)

	// 4. BuildChildrenCleanupOperations will be called with empty child resources
	// This will return empty operations
	// 5. ExecuteTransaction will be called with the delete operation
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test
	err := repo.Delete(ctx, key)
	assert.NoError(t, err)
}

func TestResourceRepository_Delete_ResourceWithOwnerReferences(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}
	resource := &sdkmeta.Object{
		ObjectKey: &key,
		ObjectMeta: &sdkmeta.ObjectMeta{
			Labels:      map[string]string{},
			Annotations: map[string]string{},
			OwnerReferences: []sdkmeta.OwnerReference{
				{
					TypeMeta: &sdkmeta.ObjectType{
						Group:     "apps",
						Version:   "v1",
						Kind:      "Deployment",
						Namespace: "default",
					},
					Name:               "parent-deployment",
					UID:                "parent-uid",
					BlockOwnerDeletion: true,
				},
				{
					TypeMeta: &sdkmeta.ObjectType{
						Group:     "apps",
						Version:   "v1",
						Kind:      "StatefulSet",
						Namespace: "default",
					},
					Name:               "another-parent",
					UID:                "another-parent-uid",
					BlockOwnerDeletion: true,
				},
			},
		},
		SystemMeta: &sdkmeta.SystemMeta{
			UID: "test-uid",
		},
		Spec: map[string]interface{}{
			"replicas": 3,
		},
	}

	// Mock expectations for resource with owner references
	mockStore.EXPECT().Get(ctx, key).Return(resource, nil)
	// Mock owner reference cleanup operations
	mockClient.EXPECT().Get(ctx, mock.Anything).Return([]byte("{}"), nil).Maybe()
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test
	err := repo.Delete(ctx, key)
	assert.NoError(t, err)
}

func TestResourceRepository_Delete_ResourceWithChildResources(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "parent-resource",
	}
	resource := &sdkmeta.Object{
		ObjectKey: &key,
		ObjectMeta: &sdkmeta.ObjectMeta{
			Labels:          map[string]string{},
			Annotations:     map[string]string{},
			OwnerReferences: []sdkmeta.OwnerReference{},
		},
		SystemMeta: &sdkmeta.SystemMeta{
			UID: "parent-uid",
		},
		Spec: map[string]interface{}{
			"replicas": 3,
		},
	}

	// Mock expectations for resource that might have child resources
	mockStore.EXPECT().Get(ctx, key).Return(resource, nil)
	// Mock child resource queries and cleanup operations
	mockClient.EXPECT().Get(ctx, mock.Anything).Return([]byte("{}"), nil).Maybe()
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test
	err := repo.Delete(ctx, key)
	assert.NoError(t, err)
}

func TestResourceRepository_Delete_ResourceNotFound(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "nonexistent-resource",
	}

	// Mock expectations - resource not found
	mockStore.EXPECT().Get(ctx, key).Return(nil, types.NewNotFoundError("resource not found"))

	// Test
	err := repo.Delete(ctx, key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "resource not found")
}

func TestResourceRepository_Delete_ErrorDuringOwnerReferenceCleanup(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	mockClient := mocks.NewMockClientWrapper(t)
	backoffManager := &lib.BackoffManager{}
	watchConfig := repository.WatchConfig{}

	repo := repository.NewResourceRepository(logger, mockStore, mockClient, watchConfig, backoffManager)

	ctx := context.Background()
	key := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}
	resource := &sdkmeta.Object{
		ObjectKey: &key,
		ObjectMeta: &sdkmeta.ObjectMeta{
			Labels:      map[string]string{},
			Annotations: map[string]string{},
			OwnerReferences: []sdkmeta.OwnerReference{
				{
					TypeMeta: &sdkmeta.ObjectType{
						Group:     "apps",
						Version:   "v1",
						Kind:      "Deployment",
						Namespace: "default",
					},
					Name:               "parent-deployment",
					UID:                "parent-uid",
					BlockOwnerDeletion: true,
				},
			},
		},
		SystemMeta: &sdkmeta.SystemMeta{
			UID: "test-uid",
		},
		Spec: map[string]interface{}{
			"replicas": 3,
		},
	}

	// Mock expectations - simulate error during owner reference cleanup
	mockStore.EXPECT().Get(ctx, key).Return(resource, nil)
	mockClient.EXPECT().Get(ctx, mock.Anything).Return(nil, assert.AnError)

	// Test
	err := repo.Delete(ctx, key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create owner reference deletion operations")
}
