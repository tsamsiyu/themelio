package service

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository"
)

type SchemaService interface {
	GetSchema(ctx context.Context, gvk schema.GroupVersionKind) (*apiextensions.JSONSchemaProps, error)
	ValidateResource(ctx context.Context, obj runtime.Object) error
	StoreSchema(ctx context.Context, gvk schema.GroupVersionKind, schema *apiextensions.JSONSchemaProps) error
}

type schemaService struct {
	logger *zap.Logger
	repo   repository.SchemaRepository
}

func NewSchemaService(logger *zap.Logger, repo repository.SchemaRepository) SchemaService {
	return &schemaService{
		logger: logger,
		repo:   repo,
	}
}

func (s *schemaService) GetSchema(ctx context.Context, gvk schema.GroupVersionKind) (*apiextensions.JSONSchemaProps, error) {
	return s.repo.GetSchema(ctx, gvk)
}

func (s *schemaService) ValidateResource(ctx context.Context, obj runtime.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	schema, err := s.GetSchema(ctx, gvk)
	if err != nil {
		return err
	}

	objBytes, err := json.Marshal(obj)
	if err != nil {
		return errors.NewInvalidResourceError("Invalid resource format")
	}

	var unstructuredObj unstructured.Unstructured
	if err := json.Unmarshal(objBytes, &unstructuredObj); err != nil {
		return errors.NewInvalidResourceError("Invalid resource format")
	}

	validator, _, err := validation.NewSchemaValidator(schema)
	if err != nil {
		return fmt.Errorf("failed to create validator: %w", err)
	}

	allErrs := validation.ValidateCustomResource(nil, &unstructuredObj, validator)
	if len(allErrs) > 0 {
		return errors.NewInvalidResourceError("Resource validation failed")
	}

	return nil
}

func (s *schemaService) StoreSchema(ctx context.Context, gvk schema.GroupVersionKind, schema *apiextensions.JSONSchemaProps) error {
	return s.repo.StoreSchema(ctx, gvk, schema)
}
