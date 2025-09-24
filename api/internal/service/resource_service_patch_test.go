package service

import (
	"context"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	"github.com/tsamsiyu/themelio/api/mocks"
	themeliotypes "github.com/tsamsiyu/themelio/sdk/pkg/types/crd"
)

func TestValidatePatchOperations(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := mocks.NewMockResourceRepository(t)
	mockSchema := mocks.NewMockSchemaService(t)
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
	mockRepo := mocks.NewMockResourceRepository(t)
	mockSchema := mocks.NewMockSchemaService(t)
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
	mockSchema.EXPECT().Get(ctx, "example.com", "TestResource").Return(crd, nil)

	objectKey := types.NewNamespacedObjectKey("example.com", "v1", "TestResource", "default", "test-resource")
	mockRepo.EXPECT().Get(ctx, objectKey).Return(existingResource, nil)

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
}
