package repository

import (
	"fmt"
)

// NotFoundError represents when a resource is not found
type NotFoundError struct {
	Key string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("resource %s not found", e.Key)
}

func NewNotFoundError(key string) *NotFoundError {
	return &NotFoundError{
		Key: key,
	}
}

// IsNotFoundError checks if an error is a NotFoundError
func IsNotFoundError(err error) bool {
	_, ok := err.(*NotFoundError)
	return ok
}
