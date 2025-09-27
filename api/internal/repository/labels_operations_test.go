package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/mocks"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

func TestLabelsOperations_BuildLabelsUpdateOps_NewLabels(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	labelsOpBuilder := NewLabelsOperations(mockStore, logger)

	objKey := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}

	oldLabels := map[string]string{}
	newLabels := map[string]string{
		"app":     "test-app",
		"version": "v1.0",
		"env":     "dev",
	}

	ops := labelsOpBuilder.BuildLabelsUpdateOps(objKey, oldLabels, newLabels)

	// Should have 3 put operations for the new labels
	assert.Len(t, ops, 3)

	// Check that all operations are put operations
	for _, op := range ops {
		assert.True(t, op.IsPut())
	}

	// Check that the keys are correctly formatted
	expectedKeys := []string{
		"/index/label/example.com/v1/TestResource/default/app/test-app/test-resource",
		"/index/label/example.com/v1/TestResource/default/version/v1.0/test-resource",
		"/index/label/example.com/v1/TestResource/default/env/dev/test-resource",
	}

	actualKeys := make([]string, len(ops))
	for i, op := range ops {
		actualKeys[i] = string(op.KeyBytes())
	}

	for _, expectedKey := range expectedKeys {
		assert.Contains(t, actualKeys, expectedKey)
	}
}

func TestLabelsOperations_BuildLabelsUpdateOps_RemovedLabels(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	labelsOpBuilder := NewLabelsOperations(mockStore, logger)

	objKey := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}

	oldLabels := map[string]string{
		"app":     "test-app",
		"version": "v1.0",
		"env":     "dev",
	}
	newLabels := map[string]string{}

	ops := labelsOpBuilder.BuildLabelsUpdateOps(objKey, oldLabels, newLabels)

	// Should have 3 delete operations for the removed labels
	assert.Len(t, ops, 3)

	// Check that all operations are delete operations
	for _, op := range ops {
		assert.True(t, op.IsDelete())
	}

	// Check that the keys are correctly formatted
	expectedKeys := []string{
		"/index/label/example.com/v1/TestResource/default/app/test-app/test-resource",
		"/index/label/example.com/v1/TestResource/default/version/v1.0/test-resource",
		"/index/label/example.com/v1/TestResource/default/env/dev/test-resource",
	}

	actualKeys := make([]string, len(ops))
	for i, op := range ops {
		actualKeys[i] = string(op.KeyBytes())
	}

	for _, expectedKey := range expectedKeys {
		assert.Contains(t, actualKeys, expectedKey)
	}
}

func TestLabelsOperations_BuildLabelsUpdateOps_MixedChanges(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	labelsOpBuilder := NewLabelsOperations(mockStore, logger)

	objKey := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}

	oldLabels := map[string]string{
		"app":     "test-app",
		"version": "v1.0",
		"env":     "dev",
	}
	newLabels := map[string]string{
		"app":     "test-app", // unchanged
		"version": "v2.0",     // changed
		// "env": "dev" removed
		"tier": "frontend", // added
	}

	ops := labelsOpBuilder.BuildLabelsUpdateOps(objKey, oldLabels, newLabels)

	// Should have 4 operations: 2 delete (env, version), 2 put (tier, version)
	assert.Len(t, ops, 4)

	// Count operations by type
	deleteOps := 0
	putOps := 0
	for _, op := range ops {
		if op.IsDelete() {
			deleteOps++
		} else if op.IsPut() {
			putOps++
		}
	}

	assert.Equal(t, 2, deleteOps) // env label removed, version changed
	assert.Equal(t, 2, putOps)    // tier added, version changed

	// Check specific operations
	actualKeys := make([]string, len(ops))
	for i, op := range ops {
		actualKeys[i] = string(op.KeyBytes())
	}

	// Should delete the env label
	assert.Contains(t, actualKeys, "/index/label/example.com/v1/TestResource/default/env/dev/test-resource")

	// Should put the tier label
	assert.Contains(t, actualKeys, "/index/label/example.com/v1/TestResource/default/tier/frontend/test-resource")

	// Should put the version label (changed)
	assert.Contains(t, actualKeys, "/index/label/example.com/v1/TestResource/default/version/v2.0/test-resource")
}

func TestLabelsOperations_BuildLabelsUpdateOps_NoChanges(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	labelsOpBuilder := NewLabelsOperations(mockStore, logger)

	objKey := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}

	labels := map[string]string{
		"app":     "test-app",
		"version": "v1.0",
		"env":     "dev",
	}

	ops := labelsOpBuilder.BuildLabelsUpdateOps(objKey, labels, labels)

	// Should have no operations when labels are unchanged
	assert.Len(t, ops, 0)
}

func TestLabelsOperations_BuildLabelsUpdateOps_NilLabels(t *testing.T) {
	logger := zap.NewNop()
	mockStore := mocks.NewMockResourceStore(t)
	labelsOpBuilder := NewLabelsOperations(mockStore, logger)

	objKey := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}

	// Test with nil old labels
	ops := labelsOpBuilder.BuildLabelsUpdateOps(objKey, nil, map[string]string{"app": "test"})
	assert.Len(t, ops, 1)
	assert.True(t, ops[0].IsPut())

	// Test with nil new labels
	ops = labelsOpBuilder.BuildLabelsUpdateOps(objKey, map[string]string{"app": "test"}, nil)
	assert.Len(t, ops, 1)
	assert.True(t, ops[0].IsDelete())

	// Test with both nil
	ops = labelsOpBuilder.BuildLabelsUpdateOps(objKey, nil, nil)
	assert.Len(t, ops, 0)
}

func TestCalculateLabelsDiff(t *testing.T) {
	tests := []struct {
		name        string
		oldLabels   map[string]string
		newLabels   map[string]string
		expectedDel map[string]string
		expectedAdd map[string]string
	}{
		{
			name:        "no changes",
			oldLabels:   map[string]string{"app": "test"},
			newLabels:   map[string]string{"app": "test"},
			expectedDel: map[string]string{},
			expectedAdd: map[string]string{},
		},
		{
			name:        "add labels",
			oldLabels:   map[string]string{},
			newLabels:   map[string]string{"app": "test", "env": "dev"},
			expectedDel: map[string]string{},
			expectedAdd: map[string]string{"app": "test", "env": "dev"},
		},
		{
			name:        "remove labels",
			oldLabels:   map[string]string{"app": "test", "env": "dev"},
			newLabels:   map[string]string{},
			expectedDel: map[string]string{"app": "test", "env": "dev"},
			expectedAdd: map[string]string{},
		},
		{
			name:        "mixed changes",
			oldLabels:   map[string]string{"app": "test", "env": "dev", "version": "v1"},
			newLabels:   map[string]string{"app": "test", "version": "v2", "tier": "frontend"},
			expectedDel: map[string]string{"env": "dev", "version": "v1"},
			expectedAdd: map[string]string{"version": "v2", "tier": "frontend"},
		},
		{
			name:        "nil old labels",
			oldLabels:   nil,
			newLabels:   map[string]string{"app": "test"},
			expectedDel: map[string]string{},
			expectedAdd: map[string]string{"app": "test"},
		},
		{
			name:        "nil new labels",
			oldLabels:   map[string]string{"app": "test"},
			newLabels:   nil,
			expectedDel: map[string]string{"app": "test"},
			expectedAdd: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleted, created := CalculateLabelsDiff(tt.oldLabels, tt.newLabels)

			assert.Equal(t, tt.expectedDel, deleted)
			assert.Equal(t, tt.expectedAdd, created)
		})
	}
}

func TestBuildLabelIndexDbKey(t *testing.T) {
	objKey := sdkmeta.ObjectKey{
		ObjectType: sdkmeta.ObjectType{
			Group:     "example.com",
			Version:   "v1",
			Kind:      "TestResource",
			Namespace: "default",
		},
		Name: "test-resource",
	}

	key := buildLabelIndexDbKey(objKey, "app", "test-app")
	expected := "/index/label/example.com/v1/TestResource/default/app/test-app/test-resource"

	assert.Equal(t, expected, key)
}
