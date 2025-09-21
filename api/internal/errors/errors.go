package errors

// InvalidInputError represents when a resource has an invalid format
type InvalidInputError struct {
	Message string
}

func (e *InvalidInputError) Error() string {
	return e.Message
}

func NewInvalidInputError(message string) *InvalidInputError {
	return &InvalidInputError{
		Message: message,
	}
}

// MarshalingError represents when marshaling or unmarshaling operations fail
type MarshalingError struct {
	Message string
}

func (e *MarshalingError) Error() string {
	return e.Message
}

func NewMarshalingError(message string) *MarshalingError {
	return &MarshalingError{
		Message: message,
	}
}
