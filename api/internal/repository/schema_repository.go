package repository

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

type SchemaRepository interface {
	GetSchema(ctx context.Context, gvk types.GroupVersionKind) (*apiextensions.JSONSchemaProps, error)
	StoreSchema(ctx context.Context, gvk types.GroupVersionKind, schema *apiextensions.JSONSchemaProps) error
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

func (r *schemaRepository) GetSchema(ctx context.Context, gvk types.GroupVersionKind) (*apiextensions.JSONSchemaProps, error) {
	resp, err := r.etcdClient.Get(ctx, gvk.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get schema from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, NewNotFoundError(gvk.String())
	}

	var schema apiextensions.JSONSchemaProps
	if err := json.Unmarshal(resp.Kvs[0].Value, &schema); err != nil {
		return nil, internalerrors.NewMarshalingError("Failed to unmarshal schema")
	}

	return &schema, nil
}

func (r *schemaRepository) StoreSchema(ctx context.Context, gvk types.GroupVersionKind, schema *apiextensions.JSONSchemaProps) error {
	schemaData, err := json.Marshal(schema)
	if err != nil {
		return internalerrors.NewMarshalingError("Failed to marshal schema")
	}

	_, err = r.etcdClient.Put(ctx, gvk.String(), string(schemaData))
	if err != nil {
		return errors.Wrap(err, "failed to store schema in etcd")
	}

	r.logger.Info("Schema stored successfully", zap.Object("gvk", gvk))

	return nil
}
