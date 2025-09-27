package repository

import (
	"fmt"
	"time"

	"github.com/tsamsiyu/themelio/api/internal/repository/types"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type LabelsOperations struct {
	store  types.ResourceStore
	logger *zap.Logger
}

func NewLabelsOperations(store types.ResourceStore, logger *zap.Logger) *LabelsOperations {
	return &LabelsOperations{store: store, logger: logger}
}

func (o *LabelsOperations) BuildLabelsUpdateOps(
	objKey sdkmeta.ObjectKey,
	oldLabels map[string]string,
	newLabels map[string]string,
) []clientv3.Op {
	var ops []clientv3.Op

	deleted, created := CalculateLabelsDiff(oldLabels, newLabels)

	for label, value := range deleted {
		dbKey := buildLabelIndexDbKey(objKey, label, value)
		ops = append(ops, clientv3.OpDelete(dbKey))
	}

	now := time.Now().Format(time.RFC3339)
	for label, value := range created {
		dbKey := buildLabelIndexDbKey(objKey, label, value)
		ops = append(ops, clientv3.OpPut(dbKey, now))
	}

	return ops
}

func (o *LabelsOperations) BuildLabelsCleanupOps(objKey sdkmeta.ObjectKey, labels map[string]string) []clientv3.Op {
	var ops []clientv3.Op

	if labels == nil {
		return ops
	}

	for label, value := range labels {
		dbKey := buildLabelIndexDbKey(objKey, label, value)
		ops = append(ops, clientv3.OpDelete(dbKey))
	}

	return ops
}

func buildLabelIndexDbKey(objKey sdkmeta.ObjectKey, label string, labelValue string) string {
	typeKey := objectTypeToDbKey(&objKey.ObjectType)
	return fmt.Sprintf("/index/label%s/%s/%s/%s", typeKey, label, labelValue, objKey.Name)
}

func CalculateLabelsDiff(oldLabels map[string]string, newLabels map[string]string) (map[string]string, map[string]string) {
	deleted := make(map[string]string)
	created := make(map[string]string)

	if oldLabels == nil {
		oldLabels = make(map[string]string)
	}
	if newLabels == nil {
		newLabels = make(map[string]string)
	}

	for label, oldValue := range oldLabels {
		if newValue, exists := newLabels[label]; !exists {
			deleted[label] = oldValue
		} else if oldValue != newValue {
			deleted[label] = oldValue
			created[label] = newValue
		}
	}

	for label, newValue := range newLabels {
		if _, exists := oldLabels[label]; !exists {
			created[label] = newValue
		}
	}

	return deleted, created
}
