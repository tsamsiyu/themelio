package service

import (
	"context"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/tsamsiyu/themelio/api/internal/repository"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

// mockSchemaService is a mock implementation of SchemaService for testing
type mockSchemaService struct {
	mock.Mock
}

func (m *mockSchemaService) GetSchema(ctx context.Context, gvk types.GroupVersionKind) (*apiextensions.JSONSchemaProps, error) {
	args := m.Called(ctx, gvk)
	return args.Get(0).(*apiextensions.JSONSchemaProps), args.Error(1)
}

func (m *mockSchemaService) ValidateResource(ctx context.Context, obj runtime.Object) error {
	args := m.Called(ctx, obj)
	return args.Error(0)
}

func (m *mockSchemaService) StoreSchema(ctx context.Context, gvk types.GroupVersionKind, schema *apiextensions.JSONSchemaProps) error {
	args := m.Called(ctx, gvk, schema)
	return args.Error(0)
}

// mockResourceRepository is a mock implementation of ResourceRepository for testing
type mockResourceRepository struct {
	mock.Mock
}

func (m *mockResourceRepository) Replace(ctx context.Context, key types.ObjectKey, resource *unstructured.Unstructured) error {
	args := m.Called(ctx, key, resource)
	return args.Error(0)
}

func (m *mockResourceRepository) Get(ctx context.Context, key types.ObjectKey) (*unstructured.Unstructured, error) {
	args := m.Called(ctx, key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*unstructured.Unstructured), args.Error(1)
}

func (m *mockResourceRepository) List(ctx context.Context, key types.ResourceKey, limit int) ([]*unstructured.Unstructured, error) {
	args := m.Called(ctx, key, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*unstructured.Unstructured), args.Error(1)
}

func (m *mockResourceRepository) Delete(ctx context.Context, key types.ObjectKey, markDeletionObjectKeys []types.ObjectKey, removeReferencesObjectKeys []types.ObjectKey) error {
	args := m.Called(ctx, key, markDeletionObjectKeys, removeReferencesObjectKeys)
	return args.Error(0)
}

func (m *mockResourceRepository) Watch(ctx context.Context, key types.DbKey, eventChan chan<- types.WatchEvent) error {
	args := m.Called(ctx, key, eventChan)
	return args.Error(0)
}

func (m *mockResourceRepository) MarkDeleted(ctx context.Context, key types.ObjectKey) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

func (m *mockResourceRepository) ListDeletions(ctx context.Context, lockKey string, lockExp time.Duration, batchLimit int) (*types.DeletionBatch, error) {
	args := m.Called(ctx, lockKey, lockExp, batchLimit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*types.DeletionBatch), args.Error(1)
}

func (m *mockResourceRepository) GetReversedOwnerReferences(ctx context.Context, parentKey types.ObjectKey) (types.ReversedOwnerReferenceSet, error) {
	args := m.Called(ctx, parentKey)
	if args.Get(0) == nil {
		return types.ReversedOwnerReferenceSet{}, args.Error(1)
	}
	return args.Get(0).(types.ReversedOwnerReferenceSet), args.Error(1)
}

func newMockResourceRepository() repository.ResourceRepository {
	return &mockResourceRepository{}
}

func TestValidatePatchOperations(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := newMockResourceRepository()
	mockSchema := &mockSchemaService{}
	service := NewResourceService(logger, mockRepo, mockSchema)

	validPatchData := []byte(`[
		{"op": "replace", "path": "/spec/replicas", "value": 3},
		{"op": "add", "path": "/metadata/labels/environment", "value": "production"}
	]`)
	validPatch, err := jsonpatch.DecodePatch(validPatchData)
	assert.NoError(t, err)

	err = service.(*resourceService).validatePatchOperations(validPatch)
	assert.NoError(t, err)

	invalidPatchData := []byte(`[
		{"op": "replace", "path": "/metadata/uid", "value": "67890"}
	]`)
	invalidPatch, err := jsonpatch.DecodePatch(invalidPatchData)
	assert.NoError(t, err)

	err = service.(*resourceService).validatePatchOperations(invalidPatch)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot modify sensitive field")
}

func TestPatchResource_InvalidPatch(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := newMockResourceRepository()
	mockSchema := &mockSchemaService{}
	service := NewResourceService(logger, mockRepo, mockSchema)

	ctx := context.Background()
	objectKey := types.NewObjectKey("example.com", "v1", "TestResource", "default", "test-resource")

	existingResource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "TestResource",
			"metadata": map[string]interface{}{
				"name":      "test-resource",
				"namespace": "default",
			},
			"spec": map[string]interface{}{
				"replicas": 1,
			},
		},
	}

	mockRepo.(*mockResourceRepository).On("Get", ctx, objectKey).Return(existingResource, nil)

	invalidPatchData := []byte(`[
		{
			"op": "replace",
			"path": "/spec/replicas"
			// Missing "value" field and comma
		}
	]`)

	result, err := service.PatchResource(ctx, objectKey, invalidPatchData)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to decode patch")

	mockRepo.(*mockResourceRepository).AssertExpectations(t)
}
