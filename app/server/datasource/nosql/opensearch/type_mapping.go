package opensearch

import (
	"errors"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
)

func parseMapping(
	logger *zap.Logger,
	mappings map[string]any,
) ([]*Ydb.Column, error) {
	// OpenSearch does not have a dedicated "array" data type.
	// Any field can contain zero or more elements, as long as they are of the same type.
	// To work with YDB fq-connector-go, users must explicitly indicate which fields
	// should be treated as lists (LIST). This is done by adding a "_meta" property
	// to the index. The "_meta" property is used during schema construction to identify
	// which fields should be considered as arrays (lists).
	meta := make(map[string]any)

	if metaSection, ok := mappings["_meta"].(map[string]any); ok {
		meta = metaSection
	} else {
		logger.Debug("_meta section is missing, continue with empty one")
	}

	properties, ok := mappings["properties"].(map[string]any)
	if !ok {
		availableKeys := make([]string, 0, len(mappings))
		for k := range mappings {
			availableKeys = append(availableKeys, k)
		}

		return nil, fmt.Errorf("extract 'properties' from mapping (available keys: %v)", availableKeys)
	}

	var columns []*Ydb.Column

	// OpenSearch document unique id
	// output only strings
	idColumn := &Ydb.Column{
		Name: "_id",
		Type: common.MakePrimitiveType(Ydb.Type_UTF8),
	}

	columns = append([]*Ydb.Column{idColumn}, columns...)

	keys := getSortedKeys(properties)

	for _, fieldName := range keys {
		props, ok := properties[fieldName].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid properties for field '%s': expected map[string]any", fieldName)
		}

		field, err := inferField(logger, fieldName, fieldName, props, meta)
		if err != nil {
			return nil, fmt.Errorf("infer field '%s': %w", fieldName, err)
		}

		columns = append(columns, field)
	}

	logger.Info("parsing finished", zap.Int("total_columns", len(columns)))

	return columns, nil
}

func inferField(
	logger *zap.Logger,
	fieldName string,
	qualifiedName string,
	mapping map[string]any,
	meta map[string]any,
) (*Ydb.Column, error) {
	properties, ok := mapping["properties"].(map[string]any)
	if !ok {
		return handleSimpleField(fieldName, qualifiedName, mapping, meta)
	}

	return handleStructField(logger, fieldName, qualifiedName, properties, meta)
}

func handleStructField(
	logger *zap.Logger,
	fieldName string,
	qualifiedName string,
	properties map[string]any,
	meta map[string]any,
) (*Ydb.Column, error) {
	children, err := processChildFields(logger, qualifiedName, properties, meta)
	if err != nil {
		return nil, fmt.Errorf("process struct field '%s': %w", fieldName, err)
	}

	ydbType := common.MakeOptionalType(common.MakeStructType(children))

	if metaValue, exists := meta[qualifiedName]; exists {
		ydbType, err = applyMetaAnnotation(qualifiedName, metaValue, ydbType)
		if err != nil {
			return nil, fmt.Errorf("apply meta annotation for field '%s': %w", fieldName, err)
		}
	}

	return &Ydb.Column{
		Name: fieldName,
		Type: ydbType,
	}, nil
}

func processChildFields(
	logger *zap.Logger,
	parentQualifiedName string,
	properties map[string]any,
	meta map[string]any,
) ([]*Ydb.StructMember, error) {
	var children []*Ydb.StructMember

	keys := getSortedKeys(properties)

	for _, childFieldName := range keys {
		childMapping := properties[childFieldName]

		childProps, ok := childMapping.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid properties for child field '%s'", childFieldName)
		}

		childQualifiedName := fmt.Sprintf("%s.%s", parentQualifiedName, childFieldName)

		childField, err := inferField(logger, childFieldName, childQualifiedName, childProps, meta)
		if err != nil {
			return nil, fmt.Errorf("process child field '%s': %w", childFieldName, err)
		}

		children = append(children, &Ydb.StructMember{
			Name: childField.Name,
			Type: childField.Type,
		})
	}

	return children, nil
}

func getSortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func applyMetaAnnotation(
	qualifiedName string,
	metaValue any,
	ydbType *Ydb.Type,
) (*Ydb.Type, error) {
	metaStr, ok := metaValue.(string)
	if !ok {
		return nil, fmt.Errorf("meta value for field '%s' must be string, got %T", qualifiedName, metaValue)
	}

	if metaStr != "list" {
		return nil, fmt.Errorf("unsupported meta value '%s' for field '%s'", metaStr, qualifiedName)
	}

	return common.MakeOptionalType(common.MakeListType(ydbType)), nil
}

func handleSimpleField(
	fieldName string,
	qualifiedName string,
	mapping map[string]any,
	meta map[string]any,
) (*Ydb.Column, error) {
	ydbType, err := typeMap(mapping)
	if err != nil {
		return nil, fmt.Errorf("map type for field '%s': %w", fieldName, err)
	}

	if _, exists := meta[qualifiedName]; exists {
		ydbType = common.MakeOptionalType(common.MakeListType(ydbType))
	}

	return &Ydb.Column{
		Name: fieldName,
		Type: ydbType,
	}, nil
}

func typeMap(
	mapping map[string]any,
) (*Ydb.Type, error) {
	fieldType, ok := mapping["type"].(string)
	if !ok {
		return nil, errors.New("missing 'type' in mapping")
	}

	var ydbType *Ydb.Type

	switch fieldType {
	case "integer":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT32)
	case "long":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT64)
	case "float":
		ydbType = common.MakePrimitiveType(Ydb.Type_FLOAT)
	case "double":
		ydbType = common.MakePrimitiveType(Ydb.Type_DOUBLE)
	case "boolean":
		ydbType = common.MakePrimitiveType(Ydb.Type_BOOL)
	case "keyword", "text":
		ydbType = common.MakePrimitiveType(Ydb.Type_UTF8)
	case "binary":
		ydbType = common.MakePrimitiveType(Ydb.Type_STRING)
	case "date":
		ydbType = common.MakePrimitiveType(Ydb.Type_TIMESTAMP)
	default:
		return nil, fmt.Errorf("unsupported type '%s': %w", fieldType, common.ErrDataTypeNotSupported)
	}

	return common.MakeOptionalType(ydbType), nil
}
