package service

import (
	"context"
	"encoding/json"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"go.uber.org/zap"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	sharedservice "github.com/tsamsiyu/themelio/api/internal/service/shared"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
	sdkschema "github.com/tsamsiyu/themelio/sdk/pkg/types/schema"
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
	GetResource(ctx context.Context, params Params) (*sdkmeta.Object, error)
	ListResources(ctx context.Context, params Params) ([]*sdkmeta.Object, error)
	DeleteResource(ctx context.Context, params Params) error
	PatchResource(ctx context.Context, params Params, patchData []byte) (*sdkmeta.Object, error)
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

func (s *resourceService) ReplaceResource(ctx context.Context, params Params, jsonData []byte) error {
	payload, err := s.convertJSONToObject(jsonData)
	if err != nil {
		return err
	}

	paramsWithName := params
	paramsWithName.Name = payload.ObjectKey.Name

	schema, err := s.schemaService.Get(ctx, paramsWithName.Group, paramsWithName.Kind)
	if err != nil {
		return err
	}

	if err := sharedservice.ValidateResource(payload, schema); err != nil {
		return err
	}

	if err := s.repo.Replace(ctx, payload, true); err != nil {
		return err
	}

	return nil
}

func (s *resourceService) GetResource(ctx context.Context, params Params) (*sdkmeta.Object, error) {
	schema, err := s.schemaService.Get(ctx, params.Group, params.Kind)
	if err != nil {
		return nil, err
	}

	objectKey, err := getObjectKeyFromParams(schema, &params)
	if err != nil {
		return nil, err
	}

	return s.repo.Get(ctx, objectKey)
}

func (s *resourceService) ListResources(ctx context.Context, params Params) ([]*sdkmeta.Object, error) {
	schema, err := s.schemaService.Get(ctx, params.Group, params.Kind)
	if err != nil {
		return nil, err
	}

	typeMeta, err := getObjectTypeFromParams(schema, &params)
	if err != nil {
		return nil, err
	}

	return s.repo.List(ctx, typeMeta, 0)
}

func (s *resourceService) DeleteResource(ctx context.Context, params Params) error {
	schema, err := s.schemaService.Get(ctx, params.Group, params.Kind)
	if err != nil {
		return err
	}

	objectKey, err := getObjectKeyFromParams(schema, &params)
	if err != nil {
		return err
	}

	return s.repo.MarkDeleted(ctx, objectKey)
}

func (s *resourceService) PatchResource(ctx context.Context, params Params, patchData []byte) (*sdkmeta.Object, error) {
	schema, err := s.schemaService.Get(ctx, params.Group, params.Kind)
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

	patchedResource, err := s.convertJSONToObject(patchedJSON)
	if err != nil {
		return nil, err
	}

	if err := sharedservice.ValidateResource(patchedResource, schema); err != nil {
		return nil, err
	}

	if err := s.repo.Replace(ctx, patchedResource, false); err != nil {
		return nil, err
	}

	return patchedResource, nil
}

func (s *resourceService) convertJSONToObject(jsonData []byte) (*sdkmeta.Object, error) {
	var obj sdkmeta.Object
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

func getObjectKeyFromParams(schema *sdkschema.ObjectSchema, params *Params) (sdkmeta.ObjectKey, error) {
	objType, err := getObjectTypeFromParams(schema, params)
	if err != nil {
		return sdkmeta.ObjectKey{}, err
	}

	return sdkmeta.ObjectKey{
		ObjectType: *objType,
		Name:       params.Name,
	}, nil
}

func getObjectTypeFromParams(schema *sdkschema.ObjectSchema, params *Params) (*sdkmeta.ObjectType, error) {
	if schema.Scope == sdkschema.ResourceScopeCluster {
		return &sdkmeta.ObjectType{
			Group:   params.Group,
			Version: params.Version,
			Kind:    params.Kind,
		}, nil
	}

	if params.Namespace == "" {
		return nil, internalerrors.NewInvalidInputError("namespace is required for namespaced resource")
	}

	return &sdkmeta.ObjectType{
		Group:     params.Group,
		Version:   params.Version,
		Kind:      params.Kind,
		Namespace: params.Namespace,
	}, nil
}
