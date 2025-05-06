package postgresql

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

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

func (typeMapper) SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
	var (
		ydbType *Ydb.Type
		err     error
	)

	// Reference table: https://github.com/ydb-platform/fq-connector-go/blob/main/docs/type_mapping_table.md
	switch typeName {
	case "boolean", "bool":
		ydbType = common.MakePrimitiveType(Ydb.Type_BOOL)
	case "smallint", "int2", "smallserial", "serial2":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT16)
	case "integer", "int", "int4", "serial", "serial4":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT32)
	case "bigint", "int8", "bigserial", "serial8":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT64)
	case "real", "float4":
		ydbType = common.MakePrimitiveType(Ydb.Type_FLOAT)
	case "double precision", "float8":
		ydbType = common.MakePrimitiveType(Ydb.Type_DOUBLE)
	case "bytea", "uuid":
		ydbType = common.MakePrimitiveType(Ydb.Type_STRING)
	case "character", "character varying", "text":
		ydbType = common.MakePrimitiveType(Ydb.Type_UTF8)
	case "json":
		ydbType = common.MakePrimitiveType(Ydb.Type_JSON)
	// TODO: jsonb to YDB_Json_document
	case "date":
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_DATE, rules.GetDateTimeFormat())
	// TODO: PostgreSQL `time` data type has no direct counterparts in the YDB's type system;
	// but it can be supported when the PG-compatible types are added to YDB:
	// https://st.yandex-team.ru/YQ-2285
	// case "time":
	case "timestamp without time zone":
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_TIMESTAMP, rules.GetDateTimeFormat())
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
	}

	if err != nil {
		return nil, fmt.Errorf("convert type '%s': %w", typeName, err)
	}

	// In PostgreSQL all columns are actually nullable, hence we wrap every T in Optional<T>.
	// See this issue for details: https://st.yandex-team.ru/YQ-2256
	ydbType = common.MakeOptionalType(ydbType)

	return &Ydb.Column{
		Name: columnName,
		Type: ydbType,
	}, nil
}

//nolint:gocyclo,funlen
func transformerFromOIDs(oids []uint32, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(oids))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(oids))

	for i, oid := range oids {
		switch oid {
		case pgtype.BoolOID:
			acceptors = append(acceptors, new(pgtype.Bool))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Bool)

				return appendValuePtrToArrowBuilder[bool, uint8, *array.Uint8Builder](&cast.Bool, builder, cast.Valid, cc.Bool())
			})
		case pgtype.Int2OID:
			acceptors = append(acceptors, new(pgtype.Int2))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Int2)

				return appendValuePtrToArrowBuilder[int16, int16, *array.Int16Builder](&cast.Int16, builder, cast.Valid, cc.Int16())
			})
		case pgtype.Int4OID:
			acceptors = append(acceptors, new(pgtype.Int4))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Int4)

				return appendValuePtrToArrowBuilder[int32, int32, *array.Int32Builder](&cast.Int32, builder, cast.Valid, cc.Int32())
			})
		case pgtype.Int8OID:
			acceptors = append(acceptors, new(pgtype.Int8))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Int8)

				return appendValuePtrToArrowBuilder[int64, int64, *array.Int64Builder](&cast.Int64, builder, cast.Valid, cc.Int64())
			})
		case pgtype.Float4OID:
			acceptors = append(acceptors, new(pgtype.Float4))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Float4)

				return appendValuePtrToArrowBuilder[float32, float32, *array.Float32Builder](
					&cast.Float32, builder, cast.Valid, cc.Float32())
			})
		case pgtype.Float8OID:
			acceptors = append(acceptors, new(pgtype.Float8))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Float8)

				return appendValuePtrToArrowBuilder[float64, float64, *array.Float64Builder](
					&cast.Float64, builder, cast.Valid, cc.Float64())
			})
		case pgtype.TextOID, pgtype.BPCharOID, pgtype.VarcharOID:
			acceptors = append(acceptors, new(pgtype.Text))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Text)

				return appendValuePtrToArrowBuilder[string, string, *array.StringBuilder](&cast.String, builder, cast.Valid, cc.String())
			})
		case pgtype.JSONOID:
			acceptors = append(acceptors, new(pgtype.Text))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Text)

				return appendValuePtrToArrowBuilder[string, string, *array.StringBuilder](&cast.String, builder, cast.Valid, cc.String())
			})
			// TODO: review all pgtype.json* types
		case pgtype.ByteaOID:
			acceptors = append(acceptors, new(*[]byte))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				// TODO: Bytea exists in the upstream library, but missing in jackx/pgx:
				// https://github.com/jackc/pgtype/blob/v1.14.0/bytea.go
				// https://github.com/jackc/pgx/blob/v5.3.1/pgtype/bytea.go
				// https://github.com/jackc/pgx/issues/1714
				cast := acceptor.(**[]byte)
				if *cast != nil {
					builder.(*array.BinaryBuilder).Append(**cast)
				} else {
					builder.(*array.BinaryBuilder).AppendNull()
				}

				return nil
			})
		case pgtype.DateOID:
			acceptors = append(acceptors, new(pgtype.Date))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders, func(acceptor any, builder array.Builder) error {
					cast := acceptor.(*pgtype.Date)

					return appendValuePtrToArrowBuilder[time.Time, string, *array.StringBuilder](
						&cast.Time, builder, cast.Valid, cc.DateToString())
				})
			case Ydb.Type_DATE:
				appenders = append(appenders, func(acceptor any, builder array.Builder) error {
					cast := acceptor.(*pgtype.Date)

					return appendValuePtrToArrowBuilder[time.Time, uint16, *array.Uint16Builder](
						&cast.Time, builder, cast.Valid, cc.Date())
				})
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with type oid %d: %w", ydbTypes[i], oid, common.ErrDataTypeNotSupported)
			}
		case pgtype.TimestampOID:
			acceptors = append(acceptors, new(pgtype.Timestamp))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders, func(acceptor any, builder array.Builder) error {
					cast := acceptor.(*pgtype.Timestamp)

					return appendValuePtrToArrowBuilder[time.Time, string, *array.StringBuilder](
						&cast.Time, builder, cast.Valid, cc.TimestampToString(true))
				})
			case Ydb.Type_TIMESTAMP:
				appenders = append(appenders, func(acceptor any, builder array.Builder) error {
					cast := acceptor.(*pgtype.Timestamp)

					return appendValuePtrToArrowBuilder[time.Time, uint64, *array.Uint64Builder](
						&cast.Time, builder, cast.Valid, cc.Timestamp())
				})
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with type oid %d: %w", ydbTypes[i], oid, common.ErrDataTypeNotSupported)
			}
		case pgtype.UUIDOID:
			acceptors = append(acceptors, new(*uuid.UUID))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(**uuid.UUID)
				if *cast != nil {
					builder.(*array.BinaryBuilder).Append([]byte((**cast).String()))
				} else {
					builder.(*array.BinaryBuilder).AppendNull()
				}

				return nil
			})
		default:
			return nil, fmt.Errorf("convert type OID %d: %w", oid, common.ErrDataTypeNotSupported)
		}
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
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
	if !valid {
		builder.AppendNull()

		return nil
	}

	return utils.AppendValueToArrowBuilder[IN, OUT, AB](value, builder, conv)
}

func NewTypeMapper() datasource.TypeMapper { return typeMapper{} }
