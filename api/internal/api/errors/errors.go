package errors

import "fmt"

// SerializationError represents JSON serialization/deserialization failures in API layer
type SerializationError struct {
	Operation string
	Err       error
}

func (e *SerializationError) Error() string {
	return fmt.Sprintf("API serialization failed: %s (caused by: %v)", e.Operation, e.Err)
}

func (e *SerializationError) Unwrap() error {
	return e.Err
}

func NewSerializationError(operation string, err error) *SerializationError {
	return &SerializationError{
		Operation: operation,
		Err:       err,
	}
}

// PatchError represents JSON patch operation failures
type PatchError struct {
	Operation string
	Err       error
}

func (e *PatchError) Error() string {
	return fmt.Sprintf("JSON patch failed: %s (caused by: %v)", e.Operation, e.Err)
}

func (e *PatchError) Unwrap() error {
	return e.Err
}

func NewPatchError(operation string, err error) *PatchError {
	return &PatchError{
		Operation: operation,
		Err:       err,
	}
}
