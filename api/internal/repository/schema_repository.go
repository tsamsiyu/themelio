package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	themeliotypes "github.com/tsamsiyu/themelio/sdk/pkg/types/crd"
)

type SchemaRepository interface {
	StoreCRD(ctx context.Context, crd *themeliotypes.CustomResourceDefinition) error
	GetCRD(ctx context.Context, group, kind string) (*themeliotypes.CustomResourceDefinition, error)
	DeleteCRD(ctx context.Context, group, kind string) error
	ListCRDs(ctx context.Context) ([]*themeliotypes.CustomResourceDefinition, error)
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

// Helper function for etcd key generation
func (r *schemaRepository) crdKey(group, kind string) string {
	return fmt.Sprintf("/crd/%s/%s", group, kind)
}

// CRD management methods
func (r *schemaRepository) StoreCRD(ctx context.Context, crd *themeliotypes.CustomResourceDefinition) error {
	crdData, err := json.Marshal(crd)
	if err != nil {
		return internalerrors.NewMarshalingError("Failed to marshal CRD")
	}

	key := r.crdKey(crd.Spec.Group, crd.Spec.Kind)
	_, err = r.etcdClient.Put(ctx, key, string(crdData))
	if err != nil {
		return errors.Wrap(err, "failed to store CRD in etcd")
	}

	r.logger.Info("CRD stored successfully",
		zap.String("group", crd.Spec.Group),
		zap.String("kind", crd.Spec.Kind))

	return nil
}

func (r *schemaRepository) GetCRD(ctx context.Context, group, kind string) (*themeliotypes.CustomResourceDefinition, error) {
	key := r.crdKey(group, kind)
	resp, err := r.etcdClient.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get CRD from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, types.NewNotFoundError(key)
	}

	var crd themeliotypes.CustomResourceDefinition
	if err := json.Unmarshal(resp.Kvs[0].Value, &crd); err != nil {
		return nil, internalerrors.NewMarshalingError("Failed to unmarshal CRD")
	}

	return &crd, nil
}

func (r *schemaRepository) DeleteCRD(ctx context.Context, group, kind string) error {
	key := r.crdKey(group, kind)
	_, err := r.etcdClient.Delete(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to delete CRD from etcd")
	}

	r.logger.Info("CRD deleted successfully",
		zap.String("group", group),
		zap.String("kind", kind))

	return nil
}

func (r *schemaRepository) ListCRDs(ctx context.Context) ([]*themeliotypes.CustomResourceDefinition, error) {
	resp, err := r.etcdClient.Get(ctx, "/crd/", clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "failed to list CRDs from etcd")
	}

	var crds []*themeliotypes.CustomResourceDefinition
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if strings.Count(key, "/") != 2 {
			continue
		}

		var crd themeliotypes.CustomResourceDefinition
		if err := json.Unmarshal(kv.Value, &crd); err != nil {
			r.logger.Error("Failed to unmarshal CRD", zap.String("key", key), zap.Error(err))
			continue
		}
		crds = append(crds, &crd)
	}

	return crds, nil
}
