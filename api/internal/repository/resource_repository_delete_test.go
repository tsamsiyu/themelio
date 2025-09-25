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

	// Given: A resource without owner references
	mockStore.EXPECT().Get(ctx, key).Return(resource, nil)
	indexPrefix := "/index/owner-reference//example.com/v1/TestResource/default/test-resource"
	mockClient.EXPECT().List(ctx, repository.Paging{Prefix: indexPrefix}).Return(&repository.Batch{KVs: []repository.KeyValue{}}, nil)

	// Mock etcd client and transaction
	mockEtcdClient := mocks.NewMockEtcdClientInterface(t)
	mockTxn := mocks.NewMockTxn(t)
	mockEtcdClient.EXPECT().Txn(ctx).Return(mockTxn)
	mockTxn.EXPECT().If(mock.Anything).Return(mockTxn)
	mockTxn.EXPECT().Then(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockTxn)
	mockTxn.EXPECT().Commit().Return(&clientv3.TxnResponse{Succeeded: true}, nil)
	mockClient.EXPECT().Client().Return(mockEtcdClient)

	// When: Deleting the resource
	err := repo.Delete(ctx, key, "test-lock")

	// Then: The deletion should succeed
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

	// Given: A resource with owner references
	mockStore.EXPECT().Get(ctx, key).Return(resource, nil)
	indexPrefix := "/index/owner-reference//example.com/v1/TestResource/default/test-resource"
	mockClient.EXPECT().List(ctx, repository.Paging{Prefix: indexPrefix}).Return(&repository.Batch{KVs: []repository.KeyValue{}}, nil)

	// Mock etcd client and transaction
	mockEtcdClient := mocks.NewMockEtcdClientInterface(t)
	mockTxn := mocks.NewMockTxn(t)
	mockEtcdClient.EXPECT().Txn(ctx).Return(mockTxn)
	mockTxn.EXPECT().If(mock.Anything).Return(mockTxn)
	mockTxn.EXPECT().Then(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockTxn)
	mockTxn.EXPECT().Commit().Return(&clientv3.TxnResponse{Succeeded: true}, nil)
	mockClient.EXPECT().Client().Return(mockEtcdClient)

	// When: Deleting the resource
	err := repo.Delete(ctx, key, "test-lock")

	// Then: The deletion should succeed
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

	// Given: A resource that might have child resources
	mockStore.EXPECT().Get(ctx, key).Return(resource, nil)
	indexPrefix := "/index/owner-reference//example.com/v1/TestResource/default/parent-resource"
	mockClient.EXPECT().List(ctx, repository.Paging{Prefix: indexPrefix}).Return(&repository.Batch{KVs: []repository.KeyValue{}}, nil)

	// Mock etcd client and transaction
	mockEtcdClient := mocks.NewMockEtcdClientInterface(t)
	mockTxn := mocks.NewMockTxn(t)
	mockEtcdClient.EXPECT().Txn(ctx).Return(mockTxn)
	mockTxn.EXPECT().If(mock.Anything).Return(mockTxn)
	mockTxn.EXPECT().Then(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockTxn)
	mockTxn.EXPECT().Commit().Return(&clientv3.TxnResponse{Succeeded: true}, nil)
	mockClient.EXPECT().Client().Return(mockEtcdClient)

	// When: Deleting the resource
	err := repo.Delete(ctx, key, "test-lock")

	// Then: The deletion should succeed
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

	// Given: A resource that does not exist
	mockStore.EXPECT().Get(ctx, key).Return(nil, repository.NewNotFoundError("resource not found"))

	// When: Attempting to delete the resource
	err := repo.Delete(ctx, key, "test-lock")

	// Then: An error should be returned
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

	// Given: A resource with owner references and a failing child query
	mockStore.EXPECT().Get(ctx, key).Return(resource, nil)
	indexPrefix := "/index/owner-reference//example.com/v1/TestResource/default/test-resource"
	mockClient.EXPECT().List(ctx, repository.Paging{Prefix: indexPrefix}).Return(nil, assert.AnError)

	// When: Attempting to delete the resource
	err := repo.Delete(ctx, key, "test-lock")

	// Then: An error should be returned
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to query children resources")
}
