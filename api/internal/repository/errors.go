package repository

import "fmt"

// NotFoundError represents when a resource is not found
type NotFoundError struct {
	ResourceType string
	Namespace    string
	Name         string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("resource %s/%s/%s not found", e.ResourceType, e.Namespace, e.Name)
}

func NewNotFoundError(resourceType, namespace, name string) *NotFoundError {
	return &NotFoundError{
		ResourceType: resourceType,
		Namespace:    namespace,
		Name:         name,
	}
}
