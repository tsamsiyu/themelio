package schema

type ResourceScope string

const (
	ResourceScopeCluster    ResourceScope = "Cluster"
	ResourceScopeNamespaced ResourceScope = "Namespaced"
)

type ObjectSchema struct {
	Group    string                `json:"group" validate:"required"`
	Kind     string                `json:"kind" validate:"required"`
	Scope    ResourceScope         `json:"scope" validate:"required"`
	Versions []ObjectSchemaVersion `json:"versions" validate:"required,min=1"`
}

type ObjectSchemaVersion struct {
	Name   string      `json:"name"`
	Schema interface{} `json:"schema"`
}
