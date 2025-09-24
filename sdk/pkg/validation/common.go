package validation

import (
	"fmt"
	"regexp"

	"github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
	"github.com/xeipuuv/gojsonschema"
)

const ALLOWED_STRING_LENGTH = 53

var (
	reDNS1123Subdomain = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$`)
	reVersion          = regexp.MustCompile(`^v[0-9]+((alpha|beta)[0-9]+)?$`)
	reKindCamel        = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	reNamespace        = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
)

func ValidateObjectTypeGroup(group string) error {
	if len(group) > ALLOWED_STRING_LENGTH || !reDNS1123Subdomain.MatchString(group) {
		return NewValidationError(fmt.Sprintf("invalid API group %q: must be a DNS subdomain (RFC1123) up to %d chars", group, ALLOWED_STRING_LENGTH))
	}
	return nil
}

func ValidateObjectTypeVersion(version string) error {
	if !reVersion.MatchString(version) {
		return NewValidationError(fmt.Sprintf("invalid version %q: must be in format v{X}, v{X}alpha{Y}, or v{X}beta{Y} where X and Y are numbers", version))
	}
	return nil
}

func ValidateObjectTypeKind(kind string) error {
	if !reKindCamel.MatchString(kind) {
		return NewValidationError(fmt.Sprintf("invalid kind %q: must be CamelCase, start with uppercase, and contain only letters/digits", kind))
	}
	return nil
}

func ValidateNamespace(ns string) error {
	if len(ns) > ALLOWED_STRING_LENGTH || !reNamespace.MatchString(ns) {
		return NewValidationError(fmt.Sprintf("invalid namespace %q: must be a DNS-1123 label up to %d chars", ns, ALLOWED_STRING_LENGTH))
	}
	return nil
}

func ValidateObjectType(objType *meta.ObjectType) error {
	if err := ValidateObjectTypeGroup(objType.Group); err != nil {
		return err
	}
	if err := ValidateObjectTypeVersion(objType.Version); err != nil {
		return err
	}
	if err := ValidateObjectTypeKind(objType.Kind); err != nil {
		return err
	}
	if objType.Namespace != "" {
		if err := ValidateNamespace(objType.Namespace); err != nil {
			return err
		}
	}
	return nil
}

func ValidateJSONSchema(schema interface{}) error {
	if schema == nil {
		return NewValidationError("schema cannot be nil")
	}

	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return NewValidationError("schema must be a JSON object")
	}

	if schemaType, exists := schemaMap["type"]; !exists || schemaType == nil {
		return NewValidationError("schema must have a type")
	}

	schemaLoader := gojsonschema.NewGoLoader(schema)
	_, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		return NewValidationError("invalid JSON schema: " + err.Error())
	}

	return nil
}
