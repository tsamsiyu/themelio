package service

import (
	"context"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apimachinery/pkg/runtime"

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

func TestValidatePatchOperations(t *testing.T) {
	// Setup
	logger := zap.NewNop()
	mockRepo := newMockResourceRepository()
	mockSchema := &mockSchemaService{}
	service := NewResourceService(logger, mockRepo, mockSchema)

	// Test valid patch operations
	validPatchData := []byte(`[
		{"op": "replace", "path": "/spec/replicas", "value": 3},
		{"op": "add", "path": "/metadata/labels/environment", "value": "production"}
	]`)
	validPatch, err := jsonpatch.DecodePatch(validPatchData)
	assert.NoError(t, err)

	err = service.(*resourceService).validatePatchOperations(validPatch)
	assert.NoError(t, err)

	// Test invalid patch operation (trying to modify sensitive field)
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
	// Setup
	logger := zap.NewNop()
	mockRepo := newMockResourceRepository()
	mockSchema := &mockSchemaService{}
	service := NewResourceService(logger, mockRepo, mockSchema)

	ctx := context.Background()
	objectKey := types.NewObjectKey("example.com", "v1", "TestResource", "default", "test-resource")

	// Create invalid patch data (malformed JSON)
	invalidPatchData := []byte(`[
		{
			"op": "replace",
			"path": "/spec/replicas"
			// Missing "value" field and comma
		}
	]`)

	// Execute
	result, err := service.PatchResource(ctx, objectKey, invalidPatchData)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to decode patch")
}
