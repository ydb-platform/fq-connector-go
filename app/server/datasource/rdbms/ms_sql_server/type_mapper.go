package ms_sql_server

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

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

	_ = rules

	// MS SQL Server Data Types https://learn.microsoft.com/ru-ru/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver16
	// Reference table: https://github.com/ydb-platform/fq-connector-go/blob/main/docs/type_mapping_table.md
	switch typeName {
	case "bit":
		ydbType = common.MakePrimitiveType(Ydb.Type_BOOL)
	case "tinyint":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT8)
	case "smallint":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT16)
	case "int":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT32)
	case "bigint":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT64)
	case "real":
		// Real always stores 4 bytes
		ydbType = common.MakePrimitiveType(Ydb.Type_FLOAT)
	case "float":
		// Float may store either 4 or 8 bytes
		// https://learn.microsoft.com/ru-ru/sql/t-sql/data-types/float-and-real-transact-sql?view=sql-server-ver16#remarks
		ydbType = common.MakePrimitiveType(Ydb.Type_DOUBLE)
	case "binary", "varbinary":
		ydbType = common.MakePrimitiveType(Ydb.Type_STRING)
	case "char", "varchar", "text":
		ydbType = common.MakePrimitiveType(Ydb.Type_STRING)
	case "nchar", "nvarchar", "ntext":
		ydbType = common.MakePrimitiveType(Ydb.Type_UTF8)
	case "date", "time", "smalldatetime", "datetime", "datetime2", "datetimeoffset":
		return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
	}

	if err != nil {
		return nil, fmt.Errorf("convert type '%s': %w", typeName, err)
	}

	ydbType = common.MakeOptionalType(ydbType)

	return &Ydb.Column{
		Name: columnName,
		Type: ydbType,
	}, nil
}

func transformerFromSQLTypes(types []string, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	_ = ydbTypes
	acceptors := make([]any, 0, len(types))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(types))

	for _, typeName := range types {
		switch typeName {
		case "BIT":
			acceptors = append(acceptors, new(*bool))
			appenders = append(appenders, makeAppender[bool, uint8, *array.Uint8Builder](cc.Bool()))
		case "TINYINT":
			acceptors = append(acceptors, new(*int8))
			appenders = append(appenders, makeAppender[int8, int8, *array.Int8Builder](cc.Int8()))
		case "SMALLINT":
			acceptors = append(acceptors, new(*int16))
			appenders = append(appenders, makeAppender[int16, int16, *array.Int16Builder](cc.Int16()))
		case "INT":
			acceptors = append(acceptors, new(*int32))
			appenders = append(appenders, makeAppender[int32, int32, *array.Int32Builder](cc.Int32()))
		case "BIGINT":
			acceptors = append(acceptors, new(*int64))
			appenders = append(appenders, makeAppender[int64, int64, *array.Int64Builder](cc.Int64()))
		case "REAL":
			acceptors = append(acceptors, new(*float32))
			appenders = append(appenders, makeAppender[float32, float32, *array.Float32Builder](cc.Float32()))
		case "FLOAT":
			acceptors = append(acceptors, new(*float64))
			appenders = append(appenders, makeAppender[float64, float64, *array.Float64Builder](cc.Float64()))
		case "BINARY", "VARBINARY":
			acceptors = append(acceptors, new(*[]byte))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(**[]byte)
				if *cast != nil {
					builder.(*array.BinaryBuilder).Append(**cast)
				} else {
					builder.(*array.BinaryBuilder).AppendNull()
				}

				return nil
			})
		case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "TEXT":
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, makeAppender[string, string, *array.StringBuilder](cc.String()))
		case "date", "time", "smalldatetime", "datetime", "datetime2", "datetimeoffset":
			// TODO: add date & time processing
			return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
		default:
			return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
		}
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
}

func makeAppender[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](conv conversion.ValuePtrConverter[IN, OUT]) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		return appendValueToArrowBuilder[IN, OUT, AB](acceptor, builder, conv)
	}
}

func appendValueToArrowBuilder[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT]](
	acceptor any,
	builder array.Builder,
	conv conversion.ValuePtrConverter[IN, OUT],
) error {
	cast := acceptor.(**IN)

	if *cast == nil {
		builder.AppendNull()

		return nil
	}

	value := *cast

	out, err := conv.Convert(value)
	if err != nil {
		if errors.Is(err, common.ErrValueOutOfTypeBounds) {
			// TODO: write warning to logger
			builder.AppendNull()

			return nil
		}

		return fmt.Errorf("convert value %v: %w", value, err)
	}

	//nolint:forcetypeassert
	builder.(AB).Append(out)

	// it was copied from ClickHouse, not sure if it is necessary
	*cast = nil

	return nil
}

func NewTypeMapper() datasource.TypeMapper { return typeMapper{} }
