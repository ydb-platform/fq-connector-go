package ydb

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
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

func appendValueToArrowBuilder[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT], CONV utils.ValueConverter[IN, OUT]](
	acceptor any,
	builder array.Builder,
) error {
	cast := acceptor.(**IN)

	if *cast == nil {
		builder.AppendNull()

		return nil
	}

	value := **cast

	var converter CONV

	out, err := converter.Convert(value)
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

func transformerFromSQLTypes(typeNames []string, ydbTypes []*Ydb.Type) (paging.RowTransformer[any], error) {
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

		acceptor, appender, err := makeAcceptorAndAppenderFromSQLType(typeName, ydbTypeID)
		if err != nil {
			return nil, fmt.Errorf("make transformer: %w", err)
		}

		acceptors = append(acceptors, acceptor)
		appenders = append(appenders, appender)
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
}

func makeAcceptorAndAppenderFromSQLType(typeName string, ydbTypeID Ydb.Type_PrimitiveTypeId) (any, func(acceptor any, builder array.Builder) error, error) {
	switch typeName {
	case BoolType:
		return new(*bool), appendValueToArrowBuilder[bool, uint8, *array.Uint8Builder, utils.BoolConverter], nil
	case Int8Type:
		return new(*int8), appendValueToArrowBuilder[int8, int8, *array.Int8Builder, utils.Int8Converter], nil
	case Int16Type:
		return new(*int16), appendValueToArrowBuilder[int16, int16, *array.Int16Builder, utils.Int16Converter], nil
	case Int32Type:
		return new(*int32), appendValueToArrowBuilder[int32, int32, *array.Int32Builder, utils.Int32Converter], nil
	case Int64Type:
		return new(*int64), appendValueToArrowBuilder[int64, int64, *array.Int64Builder, utils.Int64Converter], nil
	case Uint8Type:
		return new(*uint8), appendValueToArrowBuilder[uint8, uint8, *array.Uint8Builder, utils.Uint8Converter], nil
	case Uint16Type:
		return new(*uint16), appendValueToArrowBuilder[uint16, uint16, *array.Uint16Builder, utils.Uint16Converter], nil
	case Uint32Type:
		return new(*uint32), appendValueToArrowBuilder[uint32, uint32, *array.Uint32Builder, utils.Uint32Converter], nil
	case Uint64Type:
		return new(*uint64), appendValueToArrowBuilder[uint64, uint64, *array.Uint64Builder, utils.Uint64Converter], nil
	case FloatType:
		return new(*float32), appendValueToArrowBuilder[float32, float32, *array.Float32Builder, utils.Float32Converter], nil
	case DoubleType:
		return new(*float64), appendValueToArrowBuilder[float64, float64, *array.Float64Builder, utils.Float64Converter], nil
	case StringType:
		return new(*[]byte), appendValueToArrowBuilder[[]byte, []byte, *array.BinaryBuilder, utils.BytesConverter], nil
	case Utf8Type:
		return new(*string), appendValueToArrowBuilder[string, string, *array.StringBuilder, utils.StringConverter], nil
	case DateType:
		switch ydbTypeID {
		case Ydb.Type_DATE:
			return new(*time.Time), appendValueToArrowBuilder[time.Time, uint16, *array.Uint16Builder, utils.DateConverter], nil
		case Ydb.Type_UTF8:
			return new(*time.Time), appendValueToArrowBuilder[time.Time, string, *array.StringBuilder, utils.DateToStringConverter], nil
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type id %v with sql type %s: %w", ydbTypeID, typeName, common.ErrDataTypeNotSupported)
		}
	case DatetimeType:
		switch ydbTypeID {
		case Ydb.Type_DATETIME:
			return new(*time.Time), appendValueToArrowBuilder[time.Time, uint32, *array.Uint32Builder, utils.DatetimeConverter], nil
		case Ydb.Type_UTF8:
			return new(*time.Time), appendValueToArrowBuilder[time.Time, string, *array.StringBuilder, utils.DatetimeToStringConverter], nil
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type id %v with sql type %s: %w", ydbTypeID, typeName, common.ErrDataTypeNotSupported)
		}
	case TimestampType:
		switch ydbTypeID {
		case Ydb.Type_TIMESTAMP:
			return new(*time.Time), appendValueToArrowBuilder[time.Time, uint64, *array.Uint64Builder, utils.TimestampConverter], nil
		case Ydb.Type_UTF8:
			return new(*time.Time), appendValueToArrowBuilder[time.Time, string, *array.StringBuilder, utils.TimestampToStringConverter], nil
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type id %v with sql type %s: %w", ydbTypeID, typeName, common.ErrDataTypeNotSupported)
		}
	default:
		return nil, nil, fmt.Errorf("unknown type '%v'", typeName)
	}
}

func NewTypeMapper() datasource.TypeMapper {
	return typeMapper{}
}
