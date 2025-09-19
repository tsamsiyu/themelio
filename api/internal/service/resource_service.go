package service

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sharedservice "github.com/tsamsiyu/themelio/api/internal/service/shared"
)

type ResourceService interface {
	ReplaceResource(ctx context.Context, objectKey types.ObjectKey, jsonData []byte) error
	GetResource(ctx context.Context, objectKey types.ObjectKey) (*unstructured.Unstructured, error)
	ListResources(ctx context.Context, resourceKey types.ResourceKey) ([]*unstructured.Unstructured, error)
	DeleteResource(ctx context.Context, objectKey types.ObjectKey) error
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

func (s *resourceService) convertJSONToUnstructured(jsonData []byte) (*unstructured.Unstructured, error) {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(jsonData, &obj); err != nil {
		return nil, internalerrors.NewInvalidResourceError("failed to unmarshal object")
	}
	return &obj, nil
}
