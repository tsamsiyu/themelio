package repository

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sdkschema "github.com/tsamsiyu/themelio/sdk/pkg/types/schema"
)

type SchemaRepository interface {
	StoreSchema(ctx context.Context, schema *sdkschema.ObjectSchema) error
	GetSchema(ctx context.Context, group, kind string) (*sdkschema.ObjectSchema, error)
	DeleteSchema(ctx context.Context, group, kind string) error
	ListSchemas(ctx context.Context) ([]*sdkschema.ObjectSchema, error)
}

type schemaRepository struct {
	logger     *zap.Logger
	etcdClient *clientv3.Client
}

func NewSchemaRepository(logger *zap.Logger, etcdClient *clientv3.Client) SchemaRepository {
	return &schemaRepository{
		logger:     logger,
		etcdClient: etcdClient,
	}
}

// Schema management methods
func (r *schemaRepository) StoreSchema(ctx context.Context, schema *sdkschema.ObjectSchema) error {
	schemaData, err := json.Marshal(schema)
	if err != nil {
		return internalerrors.NewMarshalingError("Failed to marshal ObjectSchema")
	}

	key := schemaDbKey(schema.Group, schema.Kind)
	_, err = r.etcdClient.Put(ctx, key, string(schemaData))
	if err != nil {
		return errors.Wrap(err, "failed to store ObjectSchema in etcd")
	}

	r.logger.Info("ObjectSchema stored successfully",
		zap.String("group", schema.Group),
		zap.String("kind", schema.Kind))

	return nil
}

func (r *schemaRepository) GetSchema(ctx context.Context, group, kind string) (*sdkschema.ObjectSchema, error) {
	key := schemaDbKey(group, kind)
	resp, err := r.etcdClient.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get ObjectSchema from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, types.NewNotFoundError(key)
	}

	var schema sdkschema.ObjectSchema
	if err := json.Unmarshal(resp.Kvs[0].Value, &schema); err != nil {
		return nil, internalerrors.NewMarshalingError("Failed to unmarshal ObjectSchema")
	}

	return &schema, nil
}

func (r *schemaRepository) DeleteSchema(ctx context.Context, group, kind string) error {
	key := schemaDbKey(group, kind)
	_, err := r.etcdClient.Delete(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to delete ObjectSchema from etcd")
	}

	r.logger.Info("ObjectSchema deleted successfully",
		zap.String("group", group),
		zap.String("kind", kind))

	return nil
}

func (r *schemaRepository) ListSchemas(ctx context.Context) ([]*sdkschema.ObjectSchema, error) {
	resp, err := r.etcdClient.Get(ctx, "/schema/", clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "failed to list ObjectSchemas from etcd")
	}

	var schemas []*sdkschema.ObjectSchema
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if strings.Count(key, "/") != 2 {
			continue
		}

		var schema sdkschema.ObjectSchema
		if err := json.Unmarshal(kv.Value, &schema); err != nil {
			r.logger.Error("Failed to unmarshal ObjectSchema", zap.String("key", key), zap.Error(err))
			continue
		}
		schemas = append(schemas, &schema)
	}

	return schemas, nil
}
