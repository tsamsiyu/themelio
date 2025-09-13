# Layered Error Handling

The API follows a layered error handling pattern where each layer defines its own error types and the API layer maps them to appropriate HTTP responses.

## Error Flow

```
Repository Layer → Service Layer → API Layer → HTTP Response
     ↓                ↓              ↓
  Data errors    Business errors  HTTP errors
```

## Layer Responsibilities

### Repository Layer (`internal/repository/errors.go`)
- **Purpose**: Handle data access errors
- **Error Types**: Storage failures, serialization issues, data not found
- **Example Errors**: 
  - `NotFound` - Resource doesn't exist in storage
  - `StorageError` - ETCD connection/operation failures
  - `SerializationError` - JSON marshal/unmarshal failures

### Service Layer (`internal/service/errors.go`)
- **Purpose**: Handle business logic errors
- **Error Types**: Validation failures, business rule violations, domain errors
- **Example Errors**:
  - `ValidationError` - Input validation failures
  - `BusinessRuleError` - Domain rule violations
  - `AlreadyExistsError` - Resource conflicts

### API Layer (`internal/handlers/errors.go`)
- **Purpose**: Handle HTTP-specific errors
- **Error Types**: HTTP status codes and API-specific errors
- **Example Errors**:
  - `BadRequest` - 400 status
  - `Unauthorized` - 401 status
  - `NotFound` - 404 status
  - `Conflict` - 409 status

## Error Mapping

The `ErrorMapper` middleware (`internal/middleware/error_mapper.go`) automatically maps errors from lower layers to appropriate HTTP responses:

- **Repository errors** → Internal server errors (500) or specific HTTP codes
- **Service errors** → Bad request (400), not found (404), conflict (409), etc.
- **API errors** → Direct HTTP response

## Usage

### Adding New Errors

When you need a new error type, add it to the appropriate layer:

```go
// In repository/errors.go
func NewCustomRepositoryError(message string) *Error {
    return &Error{
        Type:    "CustomRepositoryError",
        Message: message,
    }
}

// In service/errors.go  
func NewCustomBusinessError(rule string) *Error {
    return &Error{
        Type:    "CustomBusinessError", 
        Message: fmt.Sprintf("Business rule violation: %s", rule),
    }
}

// In handlers/errors.go
func NewCustomAPIError(message string) *APIError {
    return &APIError{
        Type:    "CustomAPIError",
        Message: message,
        Code:    422, // Unprocessable Entity
    }
}
```

### Error Mapping

Update the error mapper to handle new error types:

```go
// In middleware/error_mapper.go
func mapServiceError(err *service.Error, logger *zap.Logger) *handlers.APIError {
    switch err.Type {
    case "CustomBusinessError":
        return &handlers.APIError{
            Type:    err.Type,
            Message: err.Message,
            Code:    422, // Unprocessable Entity
        }
    // ... other cases
    }
}
```

## Benefits

1. **Separation of Concerns**: Each layer handles its own error types
2. **Consistent HTTP Responses**: Automatic mapping to appropriate status codes
3. **Extensibility**: Easy to add new error types as needed
4. **Debugging**: Clear error hierarchy with proper logging
5. **Type Safety**: Strongly typed errors prevent confusion
