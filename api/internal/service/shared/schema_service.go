package service

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
	sdkschema "github.com/tsamsiyu/themelio/sdk/pkg/types/schema"
	sdkvalidation "github.com/tsamsiyu/themelio/sdk/pkg/validation"
)

type SchemaService interface {
	// Schema management methods
	Replace(ctx context.Context, jsonData []byte) error
	Delete(ctx context.Context, group, kind string) error
	Get(ctx context.Context, group, kind string) (*sdkschema.ObjectSchema, error)
	List(ctx context.Context) ([]*sdkschema.ObjectSchema, error)
}

type schemaService struct {
	logger *zap.Logger
	repo   types.SchemaRepository
}

func NewSchemaService(logger *zap.Logger, repo types.SchemaRepository) SchemaService {
	return &schemaService{
		logger: logger,
		repo:   repo,
	}
}

func (s *schemaService) Replace(ctx context.Context, jsonData []byte) error {
	var schema sdkschema.ObjectSchema
	if err := json.Unmarshal(jsonData, &schema); err != nil {
		return internalerrors.NewInvalidInputError("failed to parse ObjectSchema JSON: " + err.Error())
	}

	if err := sdkvalidation.ValidateCRD(&schema); err != nil {
		return internalerrors.NewInvalidInputError(err.Error())
	}

	if err := s.repo.StoreSchema(ctx, &schema); err != nil {
		return err
	}

	s.logger.Info("Schema replaced successfully",
		zap.String("group", schema.Group),
		zap.String("kind", schema.Kind))

	return nil
}

// todo: do not allow deleting schemas that are in use
func (s *schemaService) Delete(ctx context.Context, group, kind string) error {
	if err := s.repo.DeleteSchema(ctx, group, kind); err != nil {
		return err
	}

	s.logger.Info("Schema deleted successfully",
		zap.String("group", group),
		zap.String("kind", kind))

	return nil
}

func (s *schemaService) Get(ctx context.Context, group, kind string) (*sdkschema.ObjectSchema, error) {
	return s.repo.GetSchema(ctx, group, kind)
}

func (s *schemaService) List(ctx context.Context) ([]*sdkschema.ObjectSchema, error) {
	return s.repo.ListSchemas(ctx)
}

func ValidateResource(obj *sdkmeta.Object, schema *sdkschema.ObjectSchema) error {
	version := obj.ObjectKey.Version

	var versionSchema interface{}
	for _, v := range schema.Versions {
		if v.Name == version {
			versionSchema = v.Schema
			break
		}
	}

	if versionSchema == nil {
		return internalerrors.NewInvalidInputError("Schema not found for version: " + version)
	}

	if err := sdkvalidation.ValidateResourceAgainstSchema(obj, versionSchema); err != nil {
		return internalerrors.NewInvalidInputError(err.Error())
	}

	return nil
}
