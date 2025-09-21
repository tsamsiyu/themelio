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
	themeliotypes "github.com/tsamsiyu/themelio/sdk/pkg/types"
)

var SENSITIVE_PATHS = []string{
	"/metadata/uid",
	"/metadata/creationTimestamp",
	"/metadata/generation",
	"/metadata/resourceVersion",
}

type Params struct {
	Group     string
	Version   string
	Kind      string
	Namespace string
	Name      string
}

type ResourceService interface {
	ReplaceResource(ctx context.Context, params Params, jsonData []byte) error
	GetResource(ctx context.Context, params Params) (*unstructured.Unstructured, error)
	ListResources(ctx context.Context, params Params) ([]*unstructured.Unstructured, error)
	DeleteResource(ctx context.Context, params Params) error
	PatchResource(ctx context.Context, params Params, patchData []byte) (*unstructured.Unstructured, error)
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

func (s *resourceService) getObjectKeyFromParams(ctx context.Context, params Params) (types.ObjectKey, error) {
	crd, err := s.schemaService.GetCRD(ctx, params.Group, params.Kind)
	if err != nil {
		return types.ObjectKey{}, err
	}

	if crd.Spec.Scope == themeliotypes.ResourceScopeCluster {
		// we just ignore namespace here as it's not needed for cluster-scoped resources
		return types.NewClusterObjectKey(params.Group, params.Version, params.Kind, params.Name), nil
	}

	if params.Namespace == "" {
		return types.ObjectKey{}, internalerrors.NewInvalidInputError("namespace is required for namespaced resource")
	}
	return types.NewNamespacedObjectKey(params.Group, params.Version, params.Kind, params.Namespace, params.Name), nil
}

func (s *resourceService) getResourceKeyFromParams(ctx context.Context, params Params) (types.ResourceKey, error) {
	crd, err := s.schemaService.GetCRD(ctx, params.Group, params.Kind)
	if err != nil {
		return types.ResourceKey{}, err
	}

	if crd.Spec.Scope == themeliotypes.ResourceScopeCluster {
		// we just ignore namespace here as it's not needed for cluster-scoped resources
		return types.NewClusterResourceKey(params.Group, params.Version, params.Kind), nil
	}

	if params.Namespace == "" {
		return types.ResourceKey{}, internalerrors.NewInvalidInputError("namespace is required for namespaced resource")
	}
	return types.NewNamespacedResourceKey(params.Group, params.Version, params.Kind, params.Namespace), nil
}

func (s *resourceService) ReplaceResource(ctx context.Context, params Params, jsonData []byte) error {
	objectKey, err := s.getObjectKeyFromParams(ctx, params)
	if err != nil {
		return err
	}

	resource, err := s.convertJSONToUnstructured(jsonData)
	if err != nil {
		return err
	}

	if err := s.schemaService.ValidateResource(ctx, resource); err != nil {
		return err
	}

	if err := s.repo.Replace(ctx, objectKey, resource); err != nil {
		return err
	}

	return nil
}

func (s *resourceService) GetResource(ctx context.Context, params Params) (*unstructured.Unstructured, error) {
	objectKey, err := s.getObjectKeyFromParams(ctx, params)
	if err != nil {
		return nil, err
	}
	return s.repo.Get(ctx, objectKey)
}

func (s *resourceService) ListResources(ctx context.Context, params Params) ([]*unstructured.Unstructured, error) {
	resourceKey, err := s.getResourceKeyFromParams(ctx, params)
	if err != nil {
		return nil, err
	}
	return s.repo.List(ctx, resourceKey, 0)
}

func (s *resourceService) DeleteResource(ctx context.Context, params Params) error {
	objectKey, err := s.getObjectKeyFromParams(ctx, params)
	if err != nil {
		return err
	}
	return s.repo.MarkDeleted(ctx, objectKey)
}

func (s *resourceService) PatchResource(ctx context.Context, params Params, patchData []byte) (*unstructured.Unstructured, error) {
	objectKey, err := s.getObjectKeyFromParams(ctx, params)
	if err != nil {
		return nil, err
	}

	existingResource, err := s.GetResource(ctx, params)
	if err != nil {
		return nil, err
	}

	existingJSON, err := json.Marshal(existingResource)
	if err != nil {
		return nil, internalerrors.NewMarshalingError("failed to marshal existing resource")
	}

	patch, err := jsonpatch.DecodePatch(patchData)
	if err != nil {
		return nil, internalerrors.NewInvalidInputError("failed to decode patch: " + err.Error())
	}

	if err := s.validatePatchOperations(patch); err != nil {
		return nil, err
	}

	patchedJSON, err := patch.Apply(existingJSON)
	if err != nil {
		return nil, internalerrors.NewInvalidInputError("failed to apply patch: " + err.Error())
	}

	patchedResource, err := s.convertJSONToUnstructured(patchedJSON)
	if err != nil {
		return nil, err
	}

	if err := s.schemaService.ValidateResource(ctx, patchedResource); err != nil {
		return nil, err
	}

	if err := s.repo.Replace(ctx, objectKey, patchedResource); err != nil {
		return nil, err
	}

	return patchedResource, nil
}

func (s *resourceService) convertJSONToUnstructured(jsonData []byte) (*unstructured.Unstructured, error) {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(jsonData, &obj); err != nil {
		return nil, internalerrors.NewInvalidInputError("failed to unmarshal object")
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
					return internalerrors.NewInvalidInputError(
						fmt.Sprintf("patch operation %d: cannot modify sensitive field %s", i, path))
				}
			}
		}
	}
	return nil
}
