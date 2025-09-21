package service

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	themeliotypes "github.com/tsamsiyu/themelio/sdk/pkg/types"
	themeliovalidation "github.com/tsamsiyu/themelio/sdk/pkg/validation"
)

type SchemaService interface {
	// CRD management methods
	Replace(ctx context.Context, jsonData []byte) error
	Delete(ctx context.Context, group, kind string) error
	Get(ctx context.Context, group, kind string) (*themeliotypes.CustomResourceDefinition, error)
	List(ctx context.Context) ([]*themeliotypes.CustomResourceDefinition, error)
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

func (s *schemaService) Replace(ctx context.Context, jsonData []byte) error {
	var crd themeliotypes.CustomResourceDefinition
	if err := json.Unmarshal(jsonData, &crd); err != nil {
		return internalerrors.NewInvalidInputError("failed to parse CRD JSON: " + err.Error())
	}

	if err := themeliovalidation.ValidateCRD(&crd); err != nil {
		return internalerrors.NewInvalidInputError(err.Error())
	}

	if err := s.repo.StoreCRD(ctx, &crd); err != nil {
		return err
	}

	s.logger.Info("CRD replaced successfully",
		zap.String("group", crd.Spec.Group),
		zap.String("kind", crd.Spec.Kind))

	return nil
}

// todo: do not allow deleting CRDs that are in use
func (s *schemaService) Delete(ctx context.Context, group, kind string) error {
	if err := s.repo.DeleteCRD(ctx, group, kind); err != nil {
		return err
	}

	s.logger.Info("CRD deleted successfully",
		zap.String("group", group),
		zap.String("kind", kind))

	return nil
}

func (s *schemaService) Get(ctx context.Context, group, kind string) (*themeliotypes.CustomResourceDefinition, error) {
	return s.repo.GetCRD(ctx, group, kind)
}

func (s *schemaService) List(ctx context.Context) ([]*themeliotypes.CustomResourceDefinition, error) {
	return s.repo.ListCRDs(ctx)
}

func ValidateResource(obj *unstructured.Unstructured, crd *themeliotypes.CustomResourceDefinition) error {
	gvk := obj.GroupVersionKind()

	var schema interface{}
	for _, version := range crd.Spec.Versions {
		if version.Name == gvk.Version {
			schema = version.Schema
			break
		}
	}

	if schema == nil {
		return internalerrors.NewInvalidInputError("Schema not found for version: " + gvk.Version)
	}

	if err := themeliovalidation.ValidateResourceAgainstSchema(obj.Object, schema); err != nil {
		return internalerrors.NewInvalidInputError(err.Error())
	}

	return nil
}
