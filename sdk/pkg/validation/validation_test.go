package validation

import (
	"testing"

	"github.com/tsamsiyu/themelio/sdk/pkg/types/crd"
)

func TestValidateCRD(t *testing.T) {
	tests := []struct {
		name    string
		crd     *crd.CustomResourceDefinition
		wantErr bool
	}{
		{
			name: "valid CRD with single version",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group: "example.com",
					Kind:  "User",
					Scope: crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{
						{
							Name: "v1",
							Schema: map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"name": map[string]interface{}{
										"type": "string",
									},
								},
								"required": []string{"name"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid CRD with multiple versions",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group: "example.com",
					Kind:  "User",
					Scope: crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{
						{
							Name: "v1alpha1",
							Schema: map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"name": map[string]interface{}{
										"type": "string",
									},
								},
							},
						},
						{
							Name: "v1beta1",
							Schema: map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"name": map[string]interface{}{
										"type": "string",
									},
									"email": map[string]interface{}{
										"type": "string",
									},
								},
							},
						},
						{
							Name: "v1",
							Schema: map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"name": map[string]interface{}{
										"type": "string",
									},
									"email": map[string]interface{}{
										"type": "string",
									},
									"age": map[string]interface{}{
										"type": "integer",
									},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid group - single component",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group: "example",
					Kind:  "User",
					Scope: crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{
						{
							Name: "v1",
							Schema: map[string]interface{}{
								"type": "object",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid group - uppercase component",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group: "Example.com",
					Kind:  "User",
					Scope: crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{
						{
							Name: "v1",
							Schema: map[string]interface{}{
								"type": "object",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid kind - lowercase start",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group: "example.com",
					Kind:  "user",
					Scope: crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{
						{
							Name: "v1",
							Schema: map[string]interface{}{
								"type": "object",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid kind - special characters",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group: "example.com",
					Kind:  "User-Resource",
					Scope: crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{
						{
							Name: "v1",
							Schema: map[string]interface{}{
								"type": "object",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no versions",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group:    "example.com",
					Kind:     "User",
					Scope:    crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid version format",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group: "example.com",
					Kind:  "User",
					Scope: crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{
						{
							Name: "1.0",
							Schema: map[string]interface{}{
								"type": "object",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "version without schema",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group: "example.com",
					Kind:  "User",
					Scope: crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{
						{
							Name:   "v1",
							Schema: nil,
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid schema - missing type",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group: "example.com",
					Kind:  "User",
					Scope: crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{
						{
							Name: "v1",
							Schema: map[string]interface{}{
								"properties": map[string]interface{}{
									"name": map[string]interface{}{
										"type": "string",
									},
								},
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "valid CRD with complex schema",
			crd: &crd.CustomResourceDefinition{
				Spec: crd.CustomResourceDefinitionSpec{
					Group: "example.com",
					Kind:  "User",
					Scope: crd.ResourceScopeNamespaced,
					Versions: []crd.CustomResourceDefinitionVersion{
						{
							Name: "v1",
							Schema: map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"metadata": map[string]interface{}{
										"type": "object",
										"properties": map[string]interface{}{
											"name": map[string]interface{}{
												"type": "string",
											},
											"labels": map[string]interface{}{
												"type": "object",
												"additionalProperties": map[string]interface{}{
													"type": "string",
												},
											},
										},
									},
									"spec": map[string]interface{}{
										"type": "object",
										"properties": map[string]interface{}{
											"email": map[string]interface{}{
												"type":   "string",
												"format": "email",
											},
											"age": map[string]interface{}{
												"type":    "integer",
												"minimum": 0,
												"maximum": 150,
											},
										},
										"required": []string{"email"},
									},
								},
								"required": []string{"metadata", "spec"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCRD(tt.crd)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCRD() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateResourceAgainstSchema(t *testing.T) {
	tests := []struct {
		name     string
		resource interface{}
		schema   interface{}
		wantErr  bool
	}{
		{
			name: "valid resource against simple schema",
			resource: map[string]interface{}{
				"name": "test",
				"age":  25,
			},
			schema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{
						"type": "string",
					},
					"age": map[string]interface{}{
						"type": "integer",
					},
				},
				"required": []string{"name"},
			},
			wantErr: false,
		},
		{
			name: "invalid resource - missing required field",
			resource: map[string]interface{}{
				"age": 25,
			},
			schema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{
						"type": "string",
					},
					"age": map[string]interface{}{
						"type": "integer",
					},
				},
				"required": []string{"name"},
			},
			wantErr: true,
		},
		{
			name: "invalid resource - wrong type",
			resource: map[string]interface{}{
				"name": "test",
				"age":  "not-a-number",
			},
			schema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{
						"type": "string",
					},
					"age": map[string]interface{}{
						"type": "integer",
					},
				},
			},
			wantErr: true,
		},
		{
			name:     "nil schema",
			resource: map[string]interface{}{"test": "value"},
			schema:   nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateResourceAgainstSchema(tt.resource, tt.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateResourceAgainstSchema() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
