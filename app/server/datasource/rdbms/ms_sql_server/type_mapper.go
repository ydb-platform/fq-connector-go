package ms_sql_server

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	_ "github.com/denisenkom/go-mssqldb"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.TypeMapper = typeMapper{}

type typeMapper struct{}

func (typeMapper) SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
	var (
		ydbType *Ydb.Type
		err     error
	)

	// Reference table: https://github.com/ydb-platform/fq-connector-go/blob/main/docs/type_mapping_table.md
	switch typeName {
	case "bit":
		ydbType = common.MakePrimitiveType(Ydb.Type_BOOL)
	case "tinyint", "int":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT16)
	case "smallint":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT32)
	case "bigint":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT64)
	case "real":
		ydbType = common.MakePrimitiveType(Ydb.Type_FLOAT)
	case "float":
		ydbType = common.MakePrimitiveType(Ydb.Type_DOUBLE)
	case "binary", "varbinary":
		ydbType = common.MakePrimitiveType(Ydb.Type_STRING)
	case "char", "varchar", "nchar", "nvarchar", "text":
		ydbType = common.MakePrimitiveType(Ydb.Type_UTF8)
	case "date", "time", "smalldatetime", "datetime", "datetime2", "datetimeoffset":
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_TIMESTAMP, rules.GetDateTimeFormat())
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
	}

	if err != nil {
		return nil, fmt.Errorf("convert type '%s': %w", typeName, err)
	}

	// MS SQL Server умеет хорошо работать с Nullable columns, поэтому мы выбираем дефолтное поведение
	ydbType = common.MakeOptionalType(ydbType)

	return &Ydb.Column{
		Name: columnName,
		Type: ydbType,
	}, nil
}

//nolint:gocyclo
func transformerFromOIDs(oids []uint32, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {

	return paging.NewRowTransformer[any](nil, nil, nil), nil
}

func appendValueToArrowBuilder[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](
	value any,
	builder array.Builder,
	valid bool,
	conv conversion.ValueConverter[IN, OUT],
) error {

	return nil
}

func appendValuePtrToArrowBuilder[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](
	value any,
	builder array.Builder,
	valid bool,
	conv conversion.ValuePtrConverter[IN, OUT],
) error {

	return nil
}

func NewTypeMapper() datasource.TypeMapper { return typeMapper{} }
