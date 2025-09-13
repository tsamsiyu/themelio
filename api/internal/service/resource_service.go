package service

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	sharedservice "github.com/tsamsiyu/themelio/api/internal/service/shared"
)

type ResourceService interface {
	ReplaceResource(ctx context.Context, gvk schema.GroupVersionKind, jsonData []byte) error
	GetResource(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error)
	ListResources(ctx context.Context, gvk schema.GroupVersionKind, namespace string) ([]*unstructured.Unstructured, error)
	DeleteResource(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error
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

func (s *resourceService) ReplaceResource(ctx context.Context, gvk schema.GroupVersionKind, jsonData []byte) error {
	resource, err := s.convertJSONToUnstructured(jsonData)
	if err != nil {
		return err
	}

	name := resource.GetName()
	namespace := resource.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}

	if err := s.schemaService.ValidateResource(ctx, resource); err != nil {
		return internalerrors.NewInvalidResourceError("schema validation failed")
	}

	if err := s.repo.Replace(ctx, gvk, namespace, name, resource); err != nil {
		return err
	}

	return nil
}

func (s *resourceService) GetResource(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error) {
	return s.repo.Get(ctx, gvk, namespace, name)
}

func (s *resourceService) ListResources(ctx context.Context, gvk schema.GroupVersionKind, namespace string) ([]*unstructured.Unstructured, error) {
	return s.repo.List(ctx, gvk, namespace)
}

func (s *resourceService) DeleteResource(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error {
	return s.repo.Delete(ctx, gvk, namespace, name)
}

func (s *resourceService) convertJSONToUnstructured(jsonData []byte) (*unstructured.Unstructured, error) {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(jsonData, &obj); err != nil {
		return nil, internalerrors.NewInvalidResourceError("failed to unmarshal object")
	}
	return &obj, nil
}
