package utils //nolint:revive

import (
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/common"
)

type schemaItem struct {
	columnDescription *datasource.ColumnDescription
	ydbColumn         *Ydb.Column
	err               error // clarifies the reason for the lack of support
}

type SchemaBuilder struct {
	typeMapper          datasource.TypeMapper
	typeMappingSettings *api_service_protos.TTypeMappingSettings
	items               []*schemaItem
}

func (sb *SchemaBuilder) AddColumn(columnDescription *datasource.ColumnDescription) error {
	item := &schemaItem{
		columnDescription: columnDescription,
	}

	item.ydbColumn, item.err = sb.typeMapper.SQLTypeToYDBColumn(columnDescription, sb.typeMappingSettings)

	if item.err != nil && !errors.Is(item.err, common.ErrDataTypeNotSupported) {
		return fmt.Errorf(
			"sql type to ydb column (%s, %s): %w",
			columnDescription.Name,
			columnDescription.Type,
			item.err,
		)
	}

	sb.items = append(sb.items, item)

	return nil
}

func (sb *SchemaBuilder) Build(logger *zap.Logger) (*api_service_protos.TSchema, error) {
	if len(sb.items) == 0 {
		return nil, common.ErrTableDoesNotExist
	}

	var (
		schema      api_service_protos.TSchema
		unsupported []string
	)

	for _, item := range sb.items {
		if item.ydbColumn == nil {
			unsupported = append(
				unsupported,
				fmt.Sprintf("name='%s' type='%s' reason='%v'", item.columnDescription.Name, item.columnDescription.Type, item.err),
			)
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
