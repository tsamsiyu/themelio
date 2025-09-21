package types

import (
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
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

	Spec   CustomResourceDefinitionSpec   `json:"spec,omitempty"`
	Status CustomResourceDefinitionStatus `json:"status,omitempty"`
}

type CustomResourceDefinitionSpec struct {
	Group    string                            `json:"group"`
	Kind     string                            `json:"kind"`
	Scope    ResourceScope                     `json:"scope"`
	Versions []CustomResourceDefinitionVersion `json:"versions"`
}

type CustomResourceDefinitionVersion struct {
	Name   string                         `json:"name"`
	Schema *apiextensions.JSONSchemaProps `json:"schema"`
}

type CustomResourceDefinitionStatus struct {
}
