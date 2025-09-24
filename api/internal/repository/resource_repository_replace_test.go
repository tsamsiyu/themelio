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

	// Given: A new resource that doesn't exist yet
	mockStore.EXPECT().Get(ctx, key).Return(nil, types.NewNotFoundError("resource not found"))
	mockStore.EXPECT().BuildPutTxOp(resource).Return(clientv3.OpPut("/example.com/v1/TestResource/default/new-resource", "{}"), nil)
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// When: Creating the new resource
	err := repo.Replace(ctx, resource)

	// Then: The creation should succeed
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

	// Given: A new resource with owner references that doesn't exist yet
	mockStore.EXPECT().Get(ctx, key).Return(nil, types.NewNotFoundError("resource not found"))
	mockStore.EXPECT().BuildPutTxOp(resource).Return(clientv3.OpPut("/example.com/v1/TestResource/default/new-resource", "{}"), nil)
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// When: Creating the new resource with owner references
	err := repo.Replace(ctx, resource)

	// Then: The creation should succeed
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

	// Given: An existing resource with different owner references
	mockStore.EXPECT().Get(ctx, key).Return(existingResource, nil)
	mockStore.EXPECT().BuildPutTxOp(newResource).Return(clientv3.OpPut("/example.com/v1/TestResource/default/test-resource", "{}"), nil)
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// When: Updating the resource with new owner references
	err := repo.Replace(ctx, newResource)

	// Then: The update should succeed
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

	// Given: An existing resource with unchanged owner references
	mockStore.EXPECT().Get(ctx, key).Return(existingResource, nil)
	mockStore.EXPECT().BuildPutTxOp(newResource).Return(clientv3.OpPut("/example.com/v1/TestResource/default/test-resource", "{}"), nil)
	mockClient.EXPECT().ExecuteTransaction(ctx, mock.Anything).Return(nil)

	// When: Updating the resource without changing owner references
	err := repo.Replace(ctx, newResource)

	// Then: The update should succeed
	assert.NoError(t, err)
}
