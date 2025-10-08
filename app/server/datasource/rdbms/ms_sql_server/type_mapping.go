package ms_sql_server

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.TypeMapper = typeMapper{}

type typeMapper struct{}

//nolint:gocyclo
func (typeMapper) SQLTypeToYDBColumn(
	columnDescription *datasource.ColumnDescription,
	rules *api_service_protos.TTypeMappingSettings,
) (*Ydb.Column, error) {
	var (
		ydbType *Ydb.Type
		err     error
	)

	_ = rules

	// MS SQL Server Data Types https://learn.microsoft.com/ru-ru/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver16
	// Reference table: https://github.com/ydb-platform/fq-connector-go/blob/main/docs/type_mapping_table.md
	switch columnDescription.Type {
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
	case "binary", "varbinary", "image":
		ydbType = common.MakePrimitiveType(Ydb.Type_STRING)
	case "char", "varchar", "text", "nchar", "nvarchar", "ntext":
		ydbType = common.MakePrimitiveType(Ydb.Type_UTF8)
	case "date":
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_DATE, rules.GetDateTimeFormat())

		if err != nil {
			return nil, fmt.Errorf("make YDB date time type: %w", err)
		}
	case "smalldatetime":
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_DATETIME, rules.GetDateTimeFormat())

		if err != nil {
			return nil, fmt.Errorf("make YDB date time type: %w", err)
		}
	case "datetime", "datetime2":
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_TIMESTAMP, rules.GetDateTimeFormat())

		if err != nil {
			return nil, fmt.Errorf("make YDB date time type: %w", err)
		}
	default:
		return nil, fmt.Errorf("convert type '%s': %w", columnDescription.Type, common.ErrDataTypeNotSupported)
	}

	if err != nil {
		return nil, fmt.Errorf("convert type '%s': %w", columnDescription.Type, err)
	}

	ydbType = common.MakeOptionalType(ydbType)

	return &Ydb.Column{
		Name: columnDescription.Name,
		Type: ydbType,
	}, nil
}

//nolint:funlen,gocyclo
func transformerFromSQLTypes(types []string, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	_ = ydbTypes
	acceptors := make([]any, 0, len(types))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(types))

	for i, typeName := range types {
		switch typeName {
		case "BIT":
			acceptors = append(acceptors, new(*bool))
			appenders = append(appenders, utils.MakeAppenderNullable[bool, uint8, *array.Uint8Builder](cc.Bool()))
		case "TINYINT":
			acceptors = append(acceptors, new(*int8))
			appenders = append(appenders, utils.MakeAppenderNullable[int8, int8, *array.Int8Builder](cc.Int8()))
		case "SMALLINT":
			acceptors = append(acceptors, new(*int16))
			appenders = append(appenders, utils.MakeAppenderNullable[int16, int16, *array.Int16Builder](cc.Int16()))
		case "INT":
			acceptors = append(acceptors, new(*int32))
			appenders = append(appenders, utils.MakeAppenderNullable[int32, int32, *array.Int32Builder](cc.Int32()))
		case "BIGINT":
			acceptors = append(acceptors, new(*int64))
			appenders = append(appenders, utils.MakeAppenderNullable[int64, int64, *array.Int64Builder](cc.Int64()))
		case "REAL":
			acceptors = append(acceptors, new(*float32))
			appenders = append(appenders, utils.MakeAppenderNullable[float32, float32, *array.Float32Builder](cc.Float32()))
		case "FLOAT":
			acceptors = append(acceptors, new(*float64))
			appenders = append(appenders, utils.MakeAppenderNullable[float64, float64, *array.Float64Builder](cc.Float64()))
		case "BINARY", "VARBINARY", "IMAGE":
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
		case "CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR", "NTEXT":
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		case "DATE":
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders, utils.MakeAppenderNullable[time.Time, string, *array.StringBuilder](cc.DateToString()))
			case Ydb.Type_DATE:
				appenders = append(appenders, utils.MakeAppenderNullable[time.Time, uint16, *array.Uint16Builder](cc.Date()))
			default:
				return nil, fmt.Errorf(
					"unexpected ydb type %v for ms sql server type %v: %w",
					ydbTypes[i], types[i], common.ErrDataTypeNotSupported)
			}
		case "SMALLDATETIME":
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders, utils.MakeAppenderNullable[time.Time, string, *array.StringBuilder](cc.DatetimeToString()))
			case Ydb.Type_DATETIME:
				appenders = append(appenders, utils.MakeAppenderNullable[time.Time, uint32, *array.Uint32Builder](cc.Datetime()))
			default:
				return nil, fmt.Errorf(
					"unexpected ydb type %v for ms sql server type %v: %w",
					ydbTypes[i], types[i], common.ErrDataTypeNotSupported)
			}
		case "DATETIME", "DATETIME2":
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders,
					utils.MakeAppenderNullable[time.Time, string, *array.StringBuilder](cc.TimestampToString(true)))
			case Ydb.Type_TIMESTAMP:
				appenders = append(appenders, utils.MakeAppenderNullable[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
			default:
				return nil, fmt.Errorf(
					"unexpected ydb type %v for ms sql server type %v: %w",
					ydbTypes[i], types[i], common.ErrDataTypeNotSupported)
			}
		default:
			return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
		}
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
}

func NewTypeMapper() datasource.TypeMapper { return typeMapper{} }
