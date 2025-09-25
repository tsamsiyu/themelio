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

func TestResourceRepository_Get(t *testing.T) {
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
	expectedResource := &sdkmeta.Object{
		ObjectKey: &key,
		ObjectMeta: &sdkmeta.ObjectMeta{
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		SystemMeta: &sdkmeta.SystemMeta{
			UID: "test-uid",
		},
		Spec: map[string]interface{}{},
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
	objType := &sdkmeta.ObjectType{
		Group:     "example.com",
		Version:   "v1",
		Kind:      "TestResource",
		Namespace: "default",
	}
	expectedResources := []*sdkmeta.Object{
		{
			ObjectKey: &sdkmeta.ObjectKey{
				ObjectType: *objType,
				Name:       "test-resource-1",
			},
			ObjectMeta: &sdkmeta.ObjectMeta{
				Labels:      map[string]string{},
				Annotations: map[string]string{},
			},
			SystemMeta: &sdkmeta.SystemMeta{
				UID: "test-uid-1",
			},
			Spec: map[string]interface{}{},
		},
	}

	// Mock expectations
	mockStore.EXPECT().List(ctx, objType, 100).Return(expectedResources, nil)

	// Test
	result, err := repo.List(ctx, objType, 100)
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
		},
		SystemMeta: &sdkmeta.SystemMeta{
			UID: "test-uid",
		},
		Spec: map[string]interface{}{},
	}

	// Mock expectations - simplified to match actual behavior
	mockStore.EXPECT().Get(ctx, key).Return(resource, nil)
	mockStore.EXPECT().BuildPutTxOp(mock.Anything).Return(clientv3.OpPut("/example.com/v1/TestResource/default/test-resource", "{}"), nil)

	// Mock etcd client and transaction
	mockEtcdClient := mocks.NewMockEtcdClientInterface(t)
	mockTxn := mocks.NewMockTxn(t)
	mockEtcdClient.EXPECT().Txn(ctx).Return(mockTxn)
	mockTxn.EXPECT().Then(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockTxn)
	mockTxn.EXPECT().Commit().Return(&clientv3.TxnResponse{Succeeded: true}, nil)
	mockClient.EXPECT().Client().Return(mockEtcdClient)

	// Test
	err := repo.MarkDeleted(ctx, key)
	assert.NoError(t, err)
}
