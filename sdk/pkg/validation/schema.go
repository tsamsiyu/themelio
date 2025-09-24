package validation

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tsamsiyu/themelio/sdk/pkg/types/schema"
	"github.com/xeipuuv/gojsonschema"
)

func ValidateCRD(crd *schema.ObjectSchema) error {
	if err := ValidateObjectTypeGroup(crd.Group); err != nil {
		return err
	}

	if err := ValidateObjectTypeKind(crd.Kind); err != nil {
		return err
	}

	if len(crd.Versions) == 0 {
		return NewValidationError("CRD must have at least one version")
	}

	for i, version := range crd.Versions {
		if err := ValidateObjectTypeVersion(version.Name); err != nil {
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
