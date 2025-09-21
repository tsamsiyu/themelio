package service

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	themeliotypes "github.com/tsamsiyu/themelio/sdk/pkg/types"
)

type SchemaService interface {
	// CRD management methods
	CreateCRD(ctx context.Context, crd *themeliotypes.CustomResourceDefinition) error
	UpdateCRD(ctx context.Context, crd *themeliotypes.CustomResourceDefinition) error
	DeleteCRD(ctx context.Context, group, kind string) error
	GetCRD(ctx context.Context, group, kind string) (*themeliotypes.CustomResourceDefinition, error)
	ListCRDs(ctx context.Context) ([]*themeliotypes.CustomResourceDefinition, error)

	// Resource validation
	ValidateResource(ctx context.Context, obj *unstructured.Unstructured) error
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

func (s *schemaService) ValidateResource(ctx context.Context, obj *unstructured.Unstructured) error {
	k8sGVK := obj.GetObjectKind().GroupVersionKind()

	crd, err := s.GetCRD(ctx, k8sGVK.Group, k8sGVK.Kind)
	if err != nil {
		return err
	}

	var schema *apiextensions.JSONSchemaProps
	for _, version := range crd.Spec.Versions {
		if version.Name == k8sGVK.Version {
			schema = version.Schema
			break
		}
	}

	if schema == nil {
		return internalerrors.NewInvalidInputError("Schema not found for version: " + k8sGVK.Version)
	}

	validator, _, err := validation.NewSchemaValidator(schema)
	if err != nil {
		return fmt.Errorf("failed to create validator: %w", err)
	}

	allErrs := validation.ValidateCustomResource(nil, obj, validator)
	if len(allErrs) > 0 {
		return internalerrors.NewInvalidInputError("Resource validation failed")
	}

	return nil
}

// CRD management methods
func (s *schemaService) CreateCRD(ctx context.Context, crd *themeliotypes.CustomResourceDefinition) error {
	existing, err := s.repo.GetCRD(ctx, crd.Spec.Group, crd.Spec.Kind)
	if err == nil && existing != nil {
		return errors.New("CRD already exists")
	}

	if err := s.repo.StoreCRD(ctx, crd); err != nil {
		return err
	}

	s.logger.Info("CRD created successfully",
		zap.String("group", crd.Spec.Group),
		zap.String("kind", crd.Spec.Kind))

	return nil
}

func (s *schemaService) UpdateCRD(ctx context.Context, crd *themeliotypes.CustomResourceDefinition) error {
	if err := s.repo.StoreCRD(ctx, crd); err != nil {
		return err
	}

	s.logger.Info("CRD updated successfully",
		zap.String("group", crd.Spec.Group),
		zap.String("kind", crd.Spec.Kind))

	return nil
}

func (s *schemaService) DeleteCRD(ctx context.Context, group, kind string) error {
	if err := s.repo.DeleteCRD(ctx, group, kind); err != nil {
		return err
	}

	s.logger.Info("CRD deleted successfully",
		zap.String("group", group),
		zap.String("kind", kind))

	return nil
}

func (s *schemaService) GetCRD(ctx context.Context, group, kind string) (*themeliotypes.CustomResourceDefinition, error) {
	return s.repo.GetCRD(ctx, group, kind)
}

func (s *schemaService) ListCRDs(ctx context.Context) ([]*themeliotypes.CustomResourceDefinition, error) {
	return s.repo.ListCRDs(ctx)
}
