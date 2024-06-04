package ydb

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.TypeMapper = typeMapper{}

type typeMapper struct {
}

var isOptional = regexp.MustCompile(`Optional<(\w+)>$`)

const (
	BoolType      = "Bool"
	Int8Type      = "Int8"
	Uint8Type     = "Uint8"
	Int16Type     = "Int16"
	Uint16Type    = "Uint16"
	Int32Type     = "Int32"
	Uint32Type    = "Uint32"
	Int64Type     = "Int64"
	Uint64Type    = "Uint64"
	FloatType     = "Float"
	DoubleType    = "Double"
	StringType    = "String"
	Utf8Type      = "Utf8"
	JsonType 	  = "Json"
	DateType      = "Date"
	DatetimeType  = "Datetime"
	TimestampType = "Timestamp"
)

func (typeMapper) SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
	var (
		ydbType *Ydb.Type
		err     error
	)

	optional := false
	if matches := isOptional.FindStringSubmatch(typeName); len(matches) > 0 {
		optional = true
		typeName = matches[1]
	}

	ydbType, err = makePrimitiveTypeFromString(typeName, rules)
	if err != nil {
		return nil, fmt.Errorf("make type: %w", err)
	}

	if optional {
		ydbType = common.MakeOptionalType(ydbType)
	}

	return &Ydb.Column{Name: columnName, Type: ydbType}, nil
}

//nolint:gocyclo
func makePrimitiveTypeFromString(typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Type, error) {
	// TODO: add all types support
	// Reference table: https://ydb.yandex-team.ru/docs/yql/reference/types/
	switch typeName {
	case BoolType:
		return common.MakePrimitiveType(Ydb.Type_BOOL), nil
	case Int8Type:
		return common.MakePrimitiveType(Ydb.Type_INT8), nil
	case Uint8Type:
		return common.MakePrimitiveType(Ydb.Type_UINT8), nil
	case Int16Type:
		return common.MakePrimitiveType(Ydb.Type_INT16), nil
	case Uint16Type:
		return common.MakePrimitiveType(Ydb.Type_UINT16), nil
	case Int32Type:
		return common.MakePrimitiveType(Ydb.Type_INT32), nil
	case Uint32Type:
		return common.MakePrimitiveType(Ydb.Type_UINT32), nil
	case Int64Type:
		return common.MakePrimitiveType(Ydb.Type_INT64), nil
	case Uint64Type:
		return common.MakePrimitiveType(Ydb.Type_UINT64), nil
	case FloatType:
		return common.MakePrimitiveType(Ydb.Type_FLOAT), nil
	case DoubleType:
		return common.MakePrimitiveType(Ydb.Type_DOUBLE), nil
	case StringType:
		return common.MakePrimitiveType(Ydb.Type_STRING), nil
	case Utf8Type:
		return common.MakePrimitiveType(Ydb.Type_UTF8), nil
	case JsonType:
		return common.MakePrimitiveType(Ydb.Type_JSON), nil
	case DateType:
		return common.MakeYdbDateTimeType(Ydb.Type_DATE, rules.GetDateTimeFormat())
	case DatetimeType:
		return common.MakeYdbDateTimeType(Ydb.Type_DATETIME, rules.GetDateTimeFormat())
	case TimestampType:
		return common.MakeYdbDateTimeType(Ydb.Type_TIMESTAMP, rules.GetDateTimeFormat())
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
	}
}

func appendToBuilderWithValueConverter[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](
	conv conversion.ValueConverter[IN, OUT],
) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		doublePtr := acceptor.(**IN)

		if *doublePtr == nil {
			builder.AppendNull()

			return nil
		}

		value := **doublePtr

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

		return nil
	}
}

func appendToBuilderWithValuePtrConverter[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](
	conv conversion.ValuePtrConverter[IN, OUT],
) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		doublePtr := acceptor.(**IN)

		ptr := *doublePtr
		if ptr == nil {
			builder.AppendNull()

			return nil
		}

		out, err := conv.Convert(ptr)
		if err != nil {
			if errors.Is(err, common.ErrValueOutOfTypeBounds) {
				// TODO: write warning to logger
				builder.AppendNull()

				return nil
			}

			return fmt.Errorf("convert value %v: %w", ptr, err)
		}

		//nolint:forcetypeassert
		builder.(AB).Append(out)

		return nil
	}
}

func transformerFromSQLTypes(typeNames []string, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(typeNames))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(typeNames))

	for i, typeName := range typeNames {
		if matches := isOptional.FindStringSubmatch(typeName); len(matches) > 0 {
			typeName = matches[1]
		}

		ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
		if err != nil {
			return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
		}

		acceptor, appender, err := makeAcceptorAndAppenderFromSQLType(typeName, ydbTypeID, cc)
		if err != nil {
			return nil, fmt.Errorf("make transformer: %w", err)
		}

		acceptors = append(acceptors, acceptor)
		appenders = append(appenders, appender)
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
}

//nolint:gocyclo
func makeAcceptorAndAppenderFromSQLType(
	typeName string,
	ydbTypeID Ydb.Type_PrimitiveTypeId,
	cc conversion.Collection,
) (any, func(acceptor any, builder array.Builder) error, error) {
	switch typeName {
	case BoolType:
		return new(*bool), appendToBuilderWithValueConverter[bool, uint8, *array.Uint8Builder](cc.Bool()), nil
	case Int8Type:
		return new(*int8), appendToBuilderWithValueConverter[int8, int8, *array.Int8Builder](cc.Int8()), nil
	case Int16Type:
		return new(*int16), appendToBuilderWithValueConverter[int16, int16, *array.Int16Builder](cc.Int16()), nil
	case Int32Type:
		return new(*int32), appendToBuilderWithValueConverter[int32, int32, *array.Int32Builder](cc.Int32()), nil
	case Int64Type:
		return new(*int64), appendToBuilderWithValueConverter[int64, int64, *array.Int64Builder](cc.Int64()), nil
	case Uint8Type:
		return new(*uint8), appendToBuilderWithValueConverter[uint8, uint8, *array.Uint8Builder](cc.Uint8()), nil
	case Uint16Type:
		return new(*uint16), appendToBuilderWithValueConverter[uint16, uint16, *array.Uint16Builder](cc.Uint16()), nil
	case Uint32Type:
		return new(*uint32), appendToBuilderWithValueConverter[uint32, uint32, *array.Uint32Builder](cc.Uint32()), nil
	case Uint64Type:
		return new(*uint64), appendToBuilderWithValueConverter[uint64, uint64, *array.Uint64Builder](cc.Uint64()), nil
	case FloatType:
		return new(*float32), appendToBuilderWithValueConverter[float32, float32, *array.Float32Builder](cc.Float32()), nil
	case DoubleType:
		return new(*float64), appendToBuilderWithValueConverter[float64, float64, *array.Float64Builder](cc.Float64()), nil
	case StringType:
		return new(*[]byte), appendToBuilderWithValueConverter[[]byte, []byte, *array.BinaryBuilder](cc.Bytes()), nil
	case Utf8Type:
		return new(*string), appendToBuilderWithValueConverter[string, string, *array.StringBuilder](cc.String()), nil
	case JsonType:
		// Copy of UTF8
		return new(*string), appendToBuilderWithValueConverter[string, string, *array.StringBuilder](cc.String()), nil
	case DateType:
		switch ydbTypeID {
		case Ydb.Type_DATE:
			return new(*time.Time), appendToBuilderWithValueConverter[time.Time, uint16, *array.Uint16Builder](cc.Date()), nil
		case Ydb.Type_UTF8:
			return new(*time.Time), appendToBuilderWithValuePtrConverter[time.Time, string, *array.StringBuilder](cc.DateToString()), nil
		default:
			return nil, nil,
				fmt.Errorf("unexpected ydb type id %v with sql type %s: %w", ydbTypeID, typeName, common.ErrDataTypeNotSupported)
		}
	case DatetimeType:
		switch ydbTypeID {
		case Ydb.Type_DATETIME:
			return new(*time.Time), appendToBuilderWithValueConverter[time.Time, uint32, *array.Uint32Builder](cc.Datetime()), nil
		case Ydb.Type_UTF8:
			return new(*time.Time),
				appendToBuilderWithValuePtrConverter[time.Time, string, *array.StringBuilder](cc.DatetimeToString()),
				nil
		default:
			return nil, nil,
				fmt.Errorf("unexpected ydb type id %v with sql type %s: %w", ydbTypeID, typeName, common.ErrDataTypeNotSupported)
		}
	case TimestampType:
		switch ydbTypeID {
		case Ydb.Type_TIMESTAMP:
			return new(*time.Time), appendToBuilderWithValueConverter[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()), nil
		case Ydb.Type_UTF8:
			return new(*time.Time),
				appendToBuilderWithValuePtrConverter[time.Time, string, *array.StringBuilder](cc.TimestampToString()),
				nil
		default:
			return nil, nil,
				fmt.Errorf("unexpected ydb type id %v with sql type %s: %w", ydbTypeID, typeName, common.ErrDataTypeNotSupported)
		}
	default:
		return nil, nil, fmt.Errorf("unknown type '%v'", typeName)
	}
}

func NewTypeMapper() datasource.TypeMapper {
	return typeMapper{}
}
