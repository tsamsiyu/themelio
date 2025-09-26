package service

import (
	"context"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	servicetypes "github.com/tsamsiyu/themelio/api/internal/service/types"
	"github.com/tsamsiyu/themelio/api/mocks"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
	sdkschema "github.com/tsamsiyu/themelio/sdk/pkg/types/schema"
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
	params := servicetypes.Params{
		Group:     "example.com",
		Version:   "v1",
		Kind:      "TestResource",
		Namespace: "default",
		Name:      "test-resource",
	}

	existingResource := &sdkmeta.Object{
		ObjectKey: &sdkmeta.ObjectKey{
			ObjectType: sdkmeta.ObjectType{
				Group:     "example.com",
				Version:   "v1",
				Kind:      "TestResource",
				Namespace: "default",
			},
			Name: "test-resource",
		},
		ObjectMeta: &sdkmeta.ObjectMeta{
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		SystemMeta: &sdkmeta.SystemMeta{
			UID: "test-uid",
		},
		Spec: map[string]interface{}{
			"replicas": 1,
		},
	}

	schema := &sdkschema.ObjectSchema{
		Group: "example.com",
		Kind:  "TestResource",
		Scope: sdkschema.ResourceScopeNamespaced,
		Versions: []sdkschema.ObjectSchemaVersion{
			{
				Name: "v1",
				Schema: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"spec": map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"replicas": map[string]interface{}{
									"type": "integer",
								},
							},
						},
					},
				},
			},
		},
	}
	mockSchema.EXPECT().Get(ctx, "example.com", "TestResource").Return(schema, nil)

	objectKey := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}
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
