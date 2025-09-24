package repository_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/lib"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	"github.com/tsamsiyu/themelio/api/mocks"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

func TestResourceRepository_Replace_NewResource(t *testing.T) {
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
		Name: "new-resource",
	}
	resource := &sdkmeta.Object{
		ObjectKey: &key,
		ObjectMeta: &sdkmeta.ObjectMeta{
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		SystemMeta: &sdkmeta.SystemMeta{
			UID: "new-uid",
		},
		Spec: map[string]interface{}{
			"replicas": 3,
		},
	}

	// Mock expectations for new resource creation
	// 1. Get existing resource - should return not found
	mockStore.EXPECT().Get(ctx, key).Return(nil, types.NewNotFoundError("resource not found"))

	// 2. Build put operation for the new resource
	mockStore.EXPECT().BuildPutTxOp(resource).Return(clientv3.OpPut("/example.com/v1/TestResource/default/new-resource", "{}"), nil)

	// 3. Execute transaction - since no existing resource, no owner reference operations needed
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test - should successfully create new resource
	err := repo.Replace(ctx, resource)
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
	key := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "new-resource",
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
			UID: "new-uid",
		},
		Spec: map[string]interface{}{
			"replicas": 3,
		},
	}

	// Mock expectations for new resource creation with owner references
	// 1. Get existing resource - should return not found
	mockStore.EXPECT().Get(ctx, key).Return(nil, types.NewNotFoundError("resource not found"))

	// 2. Build put operation for the new resource
	mockStore.EXPECT().BuildPutTxOp(resource).Return(clientv3.OpPut("/example.com/v1/TestResource/default/new-resource", "{}"), nil)

	// 3. BuildDiffOperations will be called to handle owner reference creation
	// Since this is a new resource, it will create owner references
	// Mock the GetReversedOwnerReferences call that happens during BuildAddOperations
	refKey := "/ref/apps/v1/Deployment/default/parent-deployment"
	mockClient.EXPECT().Get(ctx, refKey).Return([]byte(""), nil)

	// 4. Execute transaction with owner reference operations
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test - should successfully create new resource with owner references
	err := repo.Replace(ctx, resource)
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
	key := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}

	// Existing resource with owner references
	existingResource := &sdkmeta.Object{
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
			"replicas": 1,
		},
	}

	// New resource with different owner references
	newResource := &sdkmeta.Object{
		ObjectKey: &key,
		ObjectMeta: &sdkmeta.ObjectMeta{
			Labels:      map[string]string{},
			Annotations: map[string]string{},
			OwnerReferences: []sdkmeta.OwnerReference{
				{
					TypeMeta: &sdkmeta.ObjectType{
						Group:     "apps",
						Version:   "v1",
						Kind:      "StatefulSet",
						Namespace: "default",
					},
					Name:               "new-parent",
					UID:                "new-parent-uid",
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

	// Mock expectations for updating existing resource with owner reference changes
	mockStore.EXPECT().Get(ctx, key).Return(existingResource, nil)
	mockStore.EXPECT().BuildPutTxOp(newResource).Return(clientv3.OpPut("/example.com/v1/TestResource/default/test-resource", "{}"), nil)

	// Mock owner reference operations - these are complex internal calls
	mockClient.EXPECT().Get(ctx, mock.Anything).Return([]byte("{}"), nil).Maybe()
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test
	err := repo.Replace(ctx, newResource)
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
	key := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}

	// Existing resource
	existingResource := &sdkmeta.Object{
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
			"replicas": 1,
		},
	}

	// New resource with same owner references
	newResource := &sdkmeta.Object{
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
			"replicas": 3, // Only spec changed, owner refs same
		},
	}

	// Mock expectations - no owner reference operations needed
	mockStore.EXPECT().Get(ctx, key).Return(existingResource, nil)
	mockStore.EXPECT().BuildPutTxOp(newResource).Return(clientv3.OpPut("/example.com/v1/TestResource/default/test-resource", "{}"), nil)
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// Test
	err := repo.Replace(ctx, newResource)
	assert.NoError(t, err)
}
