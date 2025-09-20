package service

import (
	"context"
	"encoding/json"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sharedservice "github.com/tsamsiyu/themelio/api/internal/service/shared"
)

var SENSITIVE_PATHS = []string{
	"/metadata/uid",
	"/metadata/creationTimestamp",
	"/metadata/generation",
	"/metadata/resourceVersion",
}

type ResourceService interface {
	ReplaceResource(ctx context.Context, objectKey types.ObjectKey, jsonData []byte) error
	GetResource(ctx context.Context, objectKey types.ObjectKey) (*unstructured.Unstructured, error)
	ListResources(ctx context.Context, resourceKey types.ResourceKey) ([]*unstructured.Unstructured, error)
	DeleteResource(ctx context.Context, objectKey types.ObjectKey) error
	PatchResource(ctx context.Context, objectKey types.ObjectKey, patchData []byte) (*unstructured.Unstructured, error)
}

type resourceService struct {
	logger        *zap.Logger
	repo          repository.ResourceRepository
	schemaService sharedservice.SchemaService
}

func NewResourceService(logger *zap.Logger, repo repository.ResourceRepository, schemaService sharedservice.SchemaService) ResourceService {
	return &resourceService{
		logger:        logger,
		repo:          repo,
		schemaService: schemaService,
	}
}

func (s *resourceService) ReplaceResource(ctx context.Context, objectKey types.ObjectKey, jsonData []byte) error {
	resource, err := s.convertJSONToUnstructured(jsonData)
	if err != nil {
		return err
	}

	if err := s.schemaService.ValidateResource(ctx, resource); err != nil {
		return internalerrors.NewInvalidResourceError("schema validation failed")
	}

	if err := s.repo.Replace(ctx, objectKey, resource); err != nil {
		return err
	}

	return nil
}

func (s *resourceService) GetResource(ctx context.Context, objectKey types.ObjectKey) (*unstructured.Unstructured, error) {
	return s.repo.Get(ctx, objectKey)
}

func (s *resourceService) ListResources(ctx context.Context, resourceKey types.ResourceKey) ([]*unstructured.Unstructured, error) {
	return s.repo.List(ctx, resourceKey)
}

func (s *resourceService) DeleteResource(ctx context.Context, objectKey types.ObjectKey) error {
	return s.repo.MarkDeleted(ctx, objectKey)
}

func (s *resourceService) PatchResource(ctx context.Context, objectKey types.ObjectKey, patchData []byte) (*unstructured.Unstructured, error) {
	existingResource, err := s.GetResource(ctx, objectKey)
	if err != nil {
		return nil, err
	}

	existingJSON, err := json.Marshal(existingResource)
	if err != nil {
		return nil, internalerrors.NewMarshalingError("failed to marshal existing resource")
	}

	patch, err := jsonpatch.DecodePatch(patchData)
	if err != nil {
		return nil, internalerrors.NewInvalidResourceError("failed to decode patch: " + err.Error())
	}

	if err := s.validatePatchOperations(patch); err != nil {
		return nil, err
	}

	patchedJSON, err := patch.Apply(existingJSON)
	if err != nil {
		return nil, internalerrors.NewInvalidResourceError("failed to apply patch: " + err.Error())
	}

	patchedResource, err := s.convertJSONToUnstructured(patchedJSON)
	if err != nil {
		return nil, err
	}

	if err := s.schemaService.ValidateResource(ctx, patchedResource); err != nil {
		return nil, internalerrors.NewInvalidResourceError("schema validation failed for patched resource")
	}

	if err := s.repo.Replace(ctx, objectKey, patchedResource); err != nil {
		return nil, err
	}

	return patchedResource, nil
}

func (s *resourceService) convertJSONToUnstructured(jsonData []byte) (*unstructured.Unstructured, error) {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(jsonData, &obj); err != nil {
		return nil, internalerrors.NewInvalidResourceError("failed to unmarshal object")
	}
	return &obj, nil
}

// validatePatchOperations validates that patch operations don't modify sensitive system fields
func (s *resourceService) validatePatchOperations(patch jsonpatch.Patch) error {
	for i, op := range patch {
		if op["path"] != nil {
			var path string
			if err := json.Unmarshal(*op["path"], &path); err != nil {
				continue
			}

			for _, sensitivePath := range SENSITIVE_PATHS {
				if path == sensitivePath {
					return internalerrors.NewInvalidResourceError(
						fmt.Sprintf("patch operation %d: cannot modify sensitive field %s", i, path))
				}
			}
		}
	}
	return nil
}
