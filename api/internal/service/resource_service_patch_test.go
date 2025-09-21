package service

import (
	"context"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsamsiyu/themelio/api/internal/repository"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	themeliotypes "github.com/tsamsiyu/themelio/sdk/pkg/types"
)

// mockSchemaService is a mock implementation of SchemaService for testing
type mockSchemaService struct {
	mock.Mock
}

// CRD management methods
func (m *mockSchemaService) Replace(ctx context.Context, jsonData []byte) error {
	args := m.Called(ctx, jsonData)
	return args.Error(0)
}

func (m *mockSchemaService) Delete(ctx context.Context, group, kind string) error {
	args := m.Called(ctx, group, kind)
	return args.Error(0)
}

func (m *mockSchemaService) Get(ctx context.Context, group, kind string) (*themeliotypes.CustomResourceDefinition, error) {
	args := m.Called(ctx, group, kind)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*themeliotypes.CustomResourceDefinition), args.Error(1)
}

func (m *mockSchemaService) List(ctx context.Context) ([]*themeliotypes.CustomResourceDefinition, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*themeliotypes.CustomResourceDefinition), args.Error(1)
}

// Resource validation
func (m *mockSchemaService) ValidateResource(ctx context.Context, obj *unstructured.Unstructured) error {
	args := m.Called(ctx, obj)
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

func (m *mockResourceRepository) Delete(ctx context.Context, key types.ObjectKey) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

func (m *mockResourceRepository) Watch(ctx context.Context, key types.ResourceKey, eventChan chan<- types.WatchEvent) error {
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
	params := Params{
		Group:     "example.com",
		Version:   "v1",
		Kind:      "TestResource",
		Namespace: "default",
		Name:      "test-resource",
	}

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

	crd := &themeliotypes.CustomResourceDefinition{
		Spec: themeliotypes.CustomResourceDefinitionSpec{
			Group: "example.com",
			Kind:  "TestResource",
			Scope: themeliotypes.ResourceScopeNamespaced,
		},
	}
	mockSchema.On("Get", ctx, "example.com", "TestResource").Return(crd, nil)

	objectKey := types.NewNamespacedObjectKey("example.com", "v1", "TestResource", "default", "test-resource")
	mockRepo.(*mockResourceRepository).On("Get", ctx, objectKey).Return(existingResource, nil)

	invalidPatchData := []byte(`[
		{
			"op": "replace",
			"path": "/spec/replicas"
			// Missing "value" field and comma
		}
	]`)

	result, err := service.PatchResource(ctx, params, invalidPatchData)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to decode patch")

	mockRepo.(*mockResourceRepository).AssertExpectations(t)
}
