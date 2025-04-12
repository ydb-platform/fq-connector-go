package opensearch

import (
	"fmt"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"
)

func parseMapping(logger *zap.Logger, mappings map[string]interface{}) ([]*Ydb.Column, error) {
	meta := make(map[string]interface{})
	if metaSection, ok := mappings["_meta"].(map[string]interface{}); ok {
		meta = metaSection
	}

	properties, ok := mappings["properties"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to extract 'properties' from mapping")
	}

	var columns []*Ydb.Column

	for fieldName, fieldProps := range properties {
		props, ok := fieldProps.(map[string]interface{})
		if !ok {
			logger.Warn(fmt.Sprintf("Skipping field '%s': invalid properties", fieldName))
			continue
		}

		field, err := inferField(logger, fieldName, fieldName, props, meta)
		if err != nil {
			logger.Warn(fmt.Sprintf("Skipping field '%s': %v", fieldName, err))
			continue
		}

		columns = append(columns, field)
	}

	logger.Info(fmt.Sprintf("Parsed %d fields", len(columns)))

	return columns, nil
}

func inferField(logger *zap.Logger, fieldName string, qualifiedName string, mapping map[string]interface{}, meta map[string]interface{}) (*Ydb.Column, error) {
	if properties, ok := mapping["properties"].(map[string]interface{}); ok {
		var children []*Ydb.StructMember

		for childFieldName, childMapping := range properties {
			childProps, ok := childMapping.(map[string]interface{})
			if !ok {
				logger.Warn(fmt.Sprintf("Skipping invalid child field '%s'", childFieldName))
				continue
			}

			childField, err := inferField(logger, childFieldName, fmt.Sprintf("%s.%s", qualifiedName, childFieldName), childProps, meta)
			if err != nil {
				return nil, fmt.Errorf("failed to infer child field '%s': %w", childFieldName, err)
			}

			children = append(children, &Ydb.StructMember{
				Name: childField.Name,
				Type: childField.Type,
			})
		}

		ydbType := common.MakeStructType(children)

		if metaValue, exists := meta[qualifiedName]; exists {
			if metaStr, ok := metaValue.(string); ok && metaStr == "list" {
				ydbType = common.MakeListType(ydbType)
			} else {
				return nil, fmt.Errorf("_meta only supports value 'list', key: %s, value: %v", qualifiedName, metaValue)
			}
		}

		return &Ydb.Column{
			Name: fieldName,
			Type: ydbType,
		}, nil
	}

	ydbType, err := typeMap(logger, mapping)
	if err != nil {
		return nil, fmt.Errorf("failed to map type for field '%s': %w", fieldName, err)
	}

	field := &Ydb.Column{
		Name: fieldName,
		Type: ydbType,
	}

	if _, exists := meta[qualifiedName]; exists {
		field.Type = common.MakeListType(field.Type)
	}

	return field, nil
}

func typeMap(logger *zap.Logger, mapping map[string]interface{}) (*Ydb.Type, error) {
	fieldType, ok := mapping["type"].(string)
	if !ok {
		return nil, fmt.Errorf("missing 'type' in mapping")
	}

	switch fieldType {
	case "integer":
		return common.MakePrimitiveType(Ydb.Type_INT32), nil
	case "long":
		return common.MakePrimitiveType(Ydb.Type_INT64), nil
	case "float":
		return common.MakePrimitiveType(Ydb.Type_FLOAT), nil
	case "double":
		return common.MakePrimitiveType(Ydb.Type_DOUBLE), nil
	case "boolean":
		return common.MakePrimitiveType(Ydb.Type_BOOL), nil
	case "keyword", "text":
		return common.MakePrimitiveType(Ydb.Type_UTF8), nil
	case "date":
		return common.MakePrimitiveType(Ydb.Type_TIMESTAMP), nil
	default:
		logger.Debug(fmt.Sprintf("Unsupported OpenSearch type: %s", fieldType))
		return nil, fmt.Errorf("unsupported type: %s", fieldType)
	}
}
