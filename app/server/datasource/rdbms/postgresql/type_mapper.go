package postgresql

import (
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
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

	// Reference table: https://wiki.yandex-team.ru/rtmapreduce/yql-streams-corner/connectors/lld-02-tipy-dannyx/
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
	case "bytea":
		ydbType = common.MakePrimitiveType(Ydb.Type_STRING)
	case "character", "character varying", "text":
		ydbType = common.MakePrimitiveType(Ydb.Type_UTF8)
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

//nolint:gocyclo
func transformerFromOIDs(oids []uint32, ydbTypes []*Ydb.Type) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(oids))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(oids))

	for i, oid := range oids {
		switch oid {
		case pgtype.BoolOID:
			acceptors = append(acceptors, new(pgtype.Bool))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Bool)

				return appendValueToArrowBuilder[bool, uint8, *array.Uint8Builder, utils.BoolConverter](cast.Bool, builder, cast.Valid)
			})
		case pgtype.Int2OID:
			acceptors = append(acceptors, new(pgtype.Int2))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Int2)

				return appendValueToArrowBuilder[int16, int16, *array.Int16Builder, utils.Int16Converter](cast.Int16, builder, cast.Valid)
			})
		case pgtype.Int4OID:
			acceptors = append(acceptors, new(pgtype.Int4))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Int4)

				return appendValueToArrowBuilder[int32, int32, *array.Int32Builder, utils.Int32Converter](cast.Int32, builder, cast.Valid)
			})
		case pgtype.Int8OID:
			acceptors = append(acceptors, new(pgtype.Int8))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Int8)

				return appendValueToArrowBuilder[int64, int64, *array.Int64Builder, utils.Int64Converter](cast.Int64, builder, cast.Valid)
			})
		case pgtype.Float4OID:
			acceptors = append(acceptors, new(pgtype.Float4))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Float4)

				return appendValueToArrowBuilder[float32, float32, *array.Float32Builder, utils.Float32Converter](
					cast.Float32, builder, cast.Valid)
			})
		case pgtype.Float8OID:
			acceptors = append(acceptors, new(pgtype.Float8))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Float8)

				return appendValueToArrowBuilder[float64, float64, *array.Float64Builder, utils.Float64Converter](
					cast.Float64, builder, cast.Valid)
			})
		case pgtype.TextOID, pgtype.BPCharOID, pgtype.VarcharOID:
			acceptors = append(acceptors, new(pgtype.Text))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Text)

				return appendValueToArrowBuilder[string, string, *array.StringBuilder, utils.StringConverter](
					cast.String, builder, cast.Valid)
			})
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

					return appendValueToArrowBuilder[time.Time, string, *array.StringBuilder, utils.DateToStringConverter](
						cast.Time, builder, cast.Valid)
				})
			case Ydb.Type_DATE:
				appenders = append(appenders, func(acceptor any, builder array.Builder) error {
					cast := acceptor.(*pgtype.Date)

					return appendValueToArrowBuilder[time.Time, uint16, *array.Uint16Builder, utils.DateConverter](
						cast.Time, builder, cast.Valid)
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

					return appendValueToArrowBuilder[
						time.Time,
						string,
						*array.StringBuilder,
						utils.TimestampToStringConverter](cast.Time, builder, cast.Valid)
				})
			case Ydb.Type_TIMESTAMP:
				appenders = append(appenders, func(acceptor any, builder array.Builder) error {
					cast := acceptor.(*pgtype.Timestamp)

					return appendValueToArrowBuilder[time.Time, uint64, *array.Uint64Builder, utils.TimestampConverter](
						cast.Time, builder, cast.Valid)
				})
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with type oid %d: %w", ydbTypes[i], oid, common.ErrDataTypeNotSupported)
			}
		default:
			return nil, fmt.Errorf("convert type OID %d: %w", oid, common.ErrDataTypeNotSupported)
		}
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
}

func appendValueToArrowBuilder[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT], CONV utils.ValueConverter[IN, OUT]](
	value any,
	builder array.Builder,
	valid bool,
) error {
	if !valid {
		builder.AppendNull()

		return nil
	}

	cast := value.(IN)

	var converter CONV

	out, err := converter.Convert(cast)
	if err != nil {
		if errors.Is(err, common.ErrValueOutOfTypeBounds) {
			// TODO: logger ?
			builder.AppendNull()

			return nil
		}

		return fmt.Errorf("convert value: %w", err)
	}

	builder.(AB).Append(out)

	return nil
}

func NewTypeMapper() datasource.TypeMapper { return typeMapper{} }
