package utils

import (
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/common"
)

type schemaItem struct {
	columnName string
	columnType string
	ydbColumn  *Ydb.Column
}

type SchemaBuilder struct {
	typeMapper          datasource.TypeMapper
	typeMappingSettings *api_service_protos.TTypeMappingSettings
	items               []*schemaItem
}

func (sb *SchemaBuilder) AddColumn(columnName, columnType string) error {
	item := &schemaItem{
		columnName: columnName,
		columnType: columnType,
	}

	var err error
	item.ydbColumn, err = sb.typeMapper.SQLTypeToYDBColumn(columnName, columnType, sb.typeMappingSettings)

	if err != nil && !errors.Is(err, common.ErrDataTypeNotSupported) {
		return fmt.Errorf("sql type to ydb column (%s, %s): %w", columnName, columnType, err)
	}

	sb.items = append(sb.items, item)

	return nil
}

func (sb *SchemaBuilder) Build(logger *zap.Logger) (*api_service_protos.TSchema, error) {
	fmt.Println("-------------Schema Builder------------------")
	fmt.Println(sb.items)
	fmt.Println("------------------------------------------")
	if len(sb.items) == 0 {
		return nil, common.ErrTableDoesNotExist
	}

	var (
		schema      api_service_protos.TSchema
		unsupported []string
	)

	for _, item := range sb.items {
		if item.ydbColumn == nil {
			unsupported = append(unsupported, fmt.Sprintf("%s %s", item.columnName, item.columnType))
		} else {
			schema.Columns = append(schema.Columns, item.ydbColumn)
		}
	}

	if len(unsupported) > 0 {
		logger.Warn(
			"the table schema was reduced because some column types are unsupported",
			zap.Strings("unsupported columns", unsupported),
		)
	}

	return &schema, nil
}

func NewSchemaBuilder(
	typeMapper datasource.TypeMapper,
	typeMappingSettings *api_service_protos.TTypeMappingSettings,
) *SchemaBuilder {
	return &SchemaBuilder{
		typeMapper:          typeMapper,
		typeMappingSettings: typeMappingSettings,
	}
}
