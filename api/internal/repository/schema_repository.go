package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apimachinery/pkg/runtime/schema"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
)

type SchemaRepository interface {
	GetSchema(ctx context.Context, gvk schema.GroupVersionKind) (*apiextensions.JSONSchemaProps, error)
	StoreSchema(ctx context.Context, gvk schema.GroupVersionKind, schema *apiextensions.JSONSchemaProps) error
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

func (r *schemaRepository) GetSchema(ctx context.Context, gvk schema.GroupVersionKind) (*apiextensions.JSONSchemaProps, error) {
	key := r.getSchemaKey(gvk)

	resp, err := r.etcdClient.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get schema from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, NewNotFoundError("schema", "", gvk.Kind)
	}

	var schema apiextensions.JSONSchemaProps
	if err := json.Unmarshal(resp.Kvs[0].Value, &schema); err != nil {
		return nil, internalerrors.NewMarshalingError("Failed to unmarshal schema")
	}

	return &schema, nil
}

func (r *schemaRepository) StoreSchema(ctx context.Context, gvk schema.GroupVersionKind, schema *apiextensions.JSONSchemaProps) error {
	key := r.getSchemaKey(gvk)

	schemaData, err := json.Marshal(schema)
	if err != nil {
		return internalerrors.NewMarshalingError("Failed to marshal schema")
	}

	_, err = r.etcdClient.Put(ctx, key, string(schemaData))
	if err != nil {
		return errors.Wrap(err, "failed to store schema in etcd")
	}

	r.logger.Info("Schema stored successfully",
		zap.String("group", gvk.Group),
		zap.String("version", gvk.Version),
		zap.String("kind", gvk.Kind))

	return nil
}

func (r *schemaRepository) getSchemaKey(gvk schema.GroupVersionKind) string {
	group := gvk.Group
	if group == "" {
		group = "core"
	}
	return fmt.Sprintf("/schemas/%s/%s/%s", group, gvk.Version, gvk.Kind)
}
