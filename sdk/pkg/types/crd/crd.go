package crd

import "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"

const (
	CRD_GROUP   = "apiextensions.themelio.io"
	CRD_VERSION = "v1"
	CRD_KIND    = "CustomResourceDefinition"
)

type ResourceScope string

const (
	ResourceScopeCluster    ResourceScope = "Cluster"
	ResourceScopeNamespaced ResourceScope = "Namespaced"
)

type CustomResourceDefinition struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`

	Spec   CustomResourceDefinitionSpec   `json:"spec,omitempty" validate:"required"`
	Status CustomResourceDefinitionStatus `json:"status,omitempty" validate:"required"`
}

type CustomResourceDefinitionSpec struct {
	Group    string                            `json:"group" validate:"required"`
	Kind     string                            `json:"kind" validate:"required"`
	Scope    ResourceScope                     `json:"scope" validate:"required"`
	Versions []CustomResourceDefinitionVersion `json:"versions" validate:"required,min=1"`
}

type CustomResourceDefinitionVersion struct {
	Name   string      `json:"name"`
	Schema interface{} `json:"schema"`
}

type CustomResourceDefinitionStatus struct {
}
