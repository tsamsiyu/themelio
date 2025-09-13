package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	internalerrors "github.com/tsamsiyu/themelio/api/internal/errors"
)

type ResourceRepository interface {
	Replace(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string, resource *unstructured.Unstructured) error
	Get(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error)
	List(ctx context.Context, gvk schema.GroupVersionKind, namespace string) ([]*unstructured.Unstructured, error)
	Delete(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error
}

type resourceRepository struct {
	logger *zap.Logger
	client *clientv3.Client
}

func NewResourceRepository(logger *zap.Logger, client *clientv3.Client) ResourceRepository {
	return &resourceRepository{
		logger: logger,
		client: client,
	}
}

func (r *resourceRepository) getKey(gvk schema.GroupVersionKind, namespace, name string) string {
	if namespace == "" {
		namespace = "default"
	}
	return fmt.Sprintf("/%s/%s/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind, namespace, name)
}

func (r *resourceRepository) Replace(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string, resource *unstructured.Unstructured) error {
	key := r.getKey(gvk, namespace, name)

	data, err := r.marshalResource(resource)
	if err != nil {
		return err
	}

	_, err = r.client.Put(ctx, key, data)
	if err != nil {
		return errors.Wrap(err, "failed to store resource in etcd")
	}

	r.logger.Debug("Resource stored successfully in etcd",
		zap.String("key", key),
		zap.String("group", gvk.Group),
		zap.String("version", gvk.Version),
		zap.String("kind", gvk.Kind),
		zap.String("namespace", namespace),
		zap.String("name", name))

	return nil
}

func (r *resourceRepository) Get(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error) {
	key := r.getKey(gvk, namespace, name)

	resp, err := r.client.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get resource from etcd")
	}

	if len(resp.Kvs) == 0 {
		return nil, NewNotFoundError(gvk.Kind, namespace, name)
	}

	resource, err := r.unmarshalResource(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (r *resourceRepository) List(ctx context.Context, gvk schema.GroupVersionKind, namespace string) ([]*unstructured.Unstructured, error) {
	prefix := fmt.Sprintf("/%s/%s/%s/%s/", gvk.Group, gvk.Version, gvk.Kind, namespace)

	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var resources []*unstructured.Unstructured
	for _, kv := range resp.Kvs {
		resource, err := r.unmarshalResource(kv.Value)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}

	return resources, nil
}

func (r *resourceRepository) Delete(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error {
	key := r.getKey(gvk, namespace, name)

	resp, err := r.client.Delete(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to delete resource from etcd")
	}

	if resp.Deleted == 0 {
		return NewNotFoundError(gvk.Kind, namespace, name)
	}

	r.logger.Debug("Resource deleted successfully from etcd",
		zap.String("key", key),
		zap.String("group", gvk.Group),
		zap.String("version", gvk.Version),
		zap.String("kind", gvk.Kind),
		zap.String("name", name),
		zap.String("namespace", namespace))

	return nil
}

func (r *resourceRepository) marshalResource(resource *unstructured.Unstructured) (string, error) {
	data, err := json.Marshal(resource)
	if err != nil {
		return "", internalerrors.NewMarshalingError("Failed to marshal resource")
	}
	return string(data), nil
}

func (r *resourceRepository) unmarshalResource(data []byte) (*unstructured.Unstructured, error) {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, internalerrors.NewMarshalingError("Failed to unmarshal resource")
	}

	return &obj, nil
}
