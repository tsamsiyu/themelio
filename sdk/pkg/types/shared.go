package types

type ResourceScope string

const (
	ResourceScopeCluster    ResourceScope = "Cluster"
	ResourceScopeNamespaced ResourceScope = "Namespaced"
)
