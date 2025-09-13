package errors

// InvalidResourceError represents when a resource has an invalid format
type InvalidResourceError struct {
	Message string
}

func (e *InvalidResourceError) Error() string {
	return e.Message
}

func NewInvalidResourceError(message string) *InvalidResourceError {
	return &InvalidResourceError{
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
