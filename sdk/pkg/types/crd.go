package types

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	CRD_GROUP   = "apiextensions.themelio.io"
	CRD_VERSION = "v1"
	CRD_KIND    = "CustomResourceDefinition"
)

type CustomResourceDefinition struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

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
