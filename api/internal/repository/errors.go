package repository

import (
	"fmt"

	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

// NotFoundError represents when a resource is not found
type NotFoundError struct {
	ObjectKey types.ObjectKey
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("resource %s not found", e.ObjectKey.String())
}

func NewNotFoundError(objectKey types.ObjectKey) *NotFoundError {
	return &NotFoundError{
		ObjectKey: objectKey,
	}
}
