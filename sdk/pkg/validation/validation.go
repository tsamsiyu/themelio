package validation

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/tsamsiyu/themelio/sdk/pkg/types"
	"github.com/xeipuuv/gojsonschema"
)

// ValidationError represents a validation error
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

func NewValidationError(message string) *ValidationError {
	return &ValidationError{
		Message: message,
	}
}

func ValidateKind(kind string) error {
	if kind == "" {
		return NewValidationError("kind cannot be empty")
	}

	if len(kind) > 20 {
		return NewValidationError("kind cannot be longer than 20 characters")
	}

	if !unicode.IsUpper(rune(kind[0])) {
		return NewValidationError("kind must start with an uppercase letter")
	}

	if !regexp.MustCompile(`^[a-zA-Z0-9]+$`).MatchString(kind) {
		return NewValidationError("kind can only contain alphanumeric characters")
	}

	return nil
}

func ValidateApiGroup(group string) error {
	if group == "" {
		return NewValidationError("group cannot be empty")
	}

	components := strings.Split(group, ".")
	if len(components) < 2 {
		return NewValidationError("group must contain at least 2 components separated by dots")
	}

	for i, component := range components {
		if err := ValidateTerm(component, fmt.Sprintf("group component %d", i)); err != nil {
			return err
		}
	}

	return nil
}

func ValidateVersion(version string) error {
	if version == "" {
		return NewValidationError("version cannot be empty")
	}

	// Allow v{X}alpha{Y}, v{X}beta{Y}, or v{X} where X and Y are numbers
	validVersionPattern := regexp.MustCompile(`^v\d+(alpha\d+|beta\d+)?$`)
	if !validVersionPattern.MatchString(version) {
		return NewValidationError("version must be in format v{X}, v{X}alpha{Y}, or v{X}beta{Y} where X and Y are numbers")
	}

	return nil
}

func ValidateTerm(term, termName string) error {
	if term == "" {
		return NewValidationError(termName + " cannot be empty")
	}

	if len(term) > 20 {
		return NewValidationError(termName + " cannot be longer than 20 characters")
	}

	if !regexp.MustCompile(`^[a-z][a-z0-9]*$`).MatchString(term) {
		return NewValidationError(termName + " must start with a lowercase letter and can only contain lowercase alphanumeric characters")
	}

	return nil
}

func ValidateCRD(crd *types.CustomResourceDefinition) error {
	if err := ValidateApiGroup(crd.Spec.Group); err != nil {
		return err
	}

	if err := ValidateKind(crd.Spec.Kind); err != nil {
		return err
	}

	if len(crd.Spec.Versions) == 0 {
		return NewValidationError("CRD must have at least one version")
	}

	for i, version := range crd.Spec.Versions {
		if err := ValidateVersion(version.Name); err != nil {
			return NewValidationError(fmt.Sprintf("version %d: %s", i, err.Error()))
		}

		if version.Schema == nil {
			return NewValidationError(fmt.Sprintf("version '%s' must have a schema", version.Name))
		}

		if err := ValidateJSONSchema(version.Schema); err != nil {
			return NewValidationError(fmt.Sprintf("version '%s' has invalid schema: %s", version.Name, err.Error()))
		}
	}

	return nil
}

func ValidateJSONSchema(schema interface{}) error {
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return fmt.Errorf("schema must be a JSON object")
	}

	if schemaType, exists := schemaMap["type"]; !exists || schemaType == nil {
		return fmt.Errorf("schema must have a type")
	}

	schemaLoader := gojsonschema.NewGoLoader(schema)
	_, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		return fmt.Errorf("invalid JSON schema: %s", err.Error())
	}

	return nil
}

// ValidateResourceAgainstSchema validates a resource object against a JSON schema
func ValidateResourceAgainstSchema(resource interface{}, schema interface{}) error {
	if schema == nil {
		return NewValidationError("schema cannot be nil")
	}

	resourceBytes, err := json.Marshal(resource)
	if err != nil {
		return NewValidationError("failed to marshal resource: " + err.Error())
	}

	schemaLoader := gojsonschema.NewGoLoader(schema)
	documentLoader := gojsonschema.NewBytesLoader(resourceBytes)

	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return NewValidationError("validation error: " + err.Error())
	}

	if !result.Valid() {
		var errors []string
		for _, desc := range result.Errors() {
			errors = append(errors, desc.String())
		}
		return NewValidationError("resource validation failed: " + strings.Join(errors, "; "))
	}

	return nil
}
