// Package lang implements the Mistral DSL (Domain Specific Language) parser.
// It provides structures for parsing and validating workflow definitions in YAML format.
//
// Ported from mistral-master/mistral/lang/
package lang

import (
	"fmt"
	"reflect"
)

// NotDefined is a sentinel value for undefined input fields.
var NotDefined = "__MISTRAL_NOT_DEFINED__"

// Specification is the common interface for all parsed DSL specifications.
type Specification interface {
	GetName() string
	GetDescription() string
	GetTags() []string
	ToMap() map[string]interface{}
}

// specBase contains common fields for all specs.
type specBase struct {
	name        string
	description string
	tags        []string
	raw         map[string]interface{}
}

func newSpecBase(data map[string]interface{}) specBase {
	s := specBase{raw: data}
	if v, ok := data["name"].(string); ok {
		s.name = v
	}
	if v, ok := data["description"].(string); ok {
		s.description = v
	}
	if v, ok := data["tags"].([]interface{}); ok {
		for _, t := range v {
			if tag, ok := t.(string); ok {
				s.tags = append(s.tags, tag)
			}
		}
	}
	return s
}

// ValidateStringField validates that a required string field exists.
func validateRequiredField(data map[string]interface{}, field string) error {
	if _, ok := data[field]; !ok {
		return fmt.Errorf("missing required field '%s'", field)
	}
	return nil
}

// toStringSlice converts []interface{} to []string.
func toStringSlice(v interface{}) []string {
	switch val := v.(type) {
	case string:
		return []string{val}
	case []interface{}:
		result := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return val
	}
	return nil
}

// toMapSlice converts []interface{} to []map[string]interface{}.
func toMapSlice(v interface{}) []map[string]interface{} {
	switch val := v.(type) {
	case []interface{}:
		result := make([]map[string]interface{}, 0, len(val))
		for _, item := range val {
			if m, ok := item.(map[string]interface{}); ok {
				result = append(result, m)
			}
		}
		return result
	case []map[string]interface{}:
		return val
	}
	return nil
}

// mergeMaps merges multiple maps into the first one (non-destructive for earlier maps).
func mergeMaps(maps ...map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

// deepCopyMap does a shallow+deep copy of a nested map structure.
func deepCopyMap(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		switch val := v.(type) {
		case map[string]interface{}:
			dst[k] = deepCopyMap(val)
		case []interface{}:
			dst[k] = append([]interface{}{}, val...)
		default:
			dst[k] = v
		}
	}
	return dst
}

// isNil checks if an interface{} is nil or an untyped nil.
func isNil(i interface{}) bool {
	if i == nil {
		return true
	}
	v := reflect.ValueOf(i)
	return !v.IsValid()
}
