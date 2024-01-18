package ydb

import (
	"errors"
	"fmt"
	"regexp"

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
	BoolType   = "Bool"
	Int8Type   = "Int8"
	Uint8Type  = "Uint8"
	Int16Type  = "Int16"
	Uint16Type = "Uint16"
	Int32Type  = "Int32"
	Uint32Type = "Uint32"
	Int64Type  = "Int64"
	Uint64Type = "Uint64"
	FloatType  = "Float"
	DoubleType = "Double"
	StringType = "String"
	Utf8Type   = "Utf8"
)

func (typeMapper) SQLTypeToYDBColumn(columnName, typeName string, _ *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
	var (
		ydbType *Ydb.Type
		err     error
	)

	optional := false
	if matches := isOptional.FindStringSubmatch(typeName); len(matches) > 0 {
		optional = true
		typeName = matches[1]
	}

	ydbType, err = makePrimitiveTypeFromString(typeName)
	if err != nil {
		return nil, fmt.Errorf("make type: %w", err)
	}

	if optional {
		ydbType = common.MakeOptionalType(ydbType)
	}

	return &Ydb.Column{Name: columnName, Type: ydbType}, nil
}

func makePrimitiveTypeFromString(typeName string) (*Ydb.Type, error) {
	// TODO: add all types support
	// Reference table: https://ydb.yandex-team.ru/docs/yql/reference/types/
	switch {
	case typeName == BoolType:
		return common.MakePrimitiveType(Ydb.Type_BOOL), nil
	case typeName == Int8Type:
		return common.MakePrimitiveType(Ydb.Type_INT8), nil
	case typeName == Uint8Type:
		return common.MakePrimitiveType(Ydb.Type_UINT8), nil
	case typeName == Int16Type:
		return common.MakePrimitiveType(Ydb.Type_INT16), nil
	case typeName == Uint16Type:
		return common.MakePrimitiveType(Ydb.Type_UINT16), nil
	case typeName == Int32Type:
		return common.MakePrimitiveType(Ydb.Type_INT32), nil
	case typeName == Uint32Type:
		return common.MakePrimitiveType(Ydb.Type_UINT32), nil
	case typeName == Int64Type:
		return common.MakePrimitiveType(Ydb.Type_INT64), nil
	case typeName == Uint64Type:
		return common.MakePrimitiveType(Ydb.Type_UINT64), nil
	case typeName == FloatType:
		return common.MakePrimitiveType(Ydb.Type_FLOAT), nil
	case typeName == DoubleType:
		return common.MakePrimitiveType(Ydb.Type_DOUBLE), nil
	case typeName == StringType:
		return common.MakePrimitiveType(Ydb.Type_STRING), nil
	case typeName == Utf8Type:
		return common.MakePrimitiveType(Ydb.Type_UTF8), nil
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

func transformerFromSQLTypes(typeNames []string, _ []*Ydb.Type) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(typeNames))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(typeNames))

	for _, typeName := range typeNames {
		if matches := isOptional.FindStringSubmatch(typeName); len(matches) > 0 {
			typeName = matches[1]
		}

		acceptor, appender, err := makeAcceptorAndAppenderFromSQLType(typeName)
		if err != nil {
			return nil, fmt.Errorf("make transformer: %w", err)
		}

		acceptors = append(acceptors, acceptor)
		appenders = append(appenders, appender)
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
}

func makeAcceptorAndAppenderFromSQLType(typeName string) (any, func(acceptor any, builder array.Builder) error, error) {
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
	default:
		return nil, nil, fmt.Errorf("unknown type '%v'", typeName)
	}
}

func NewTypeMapper() datasource.TypeMapper {
	return typeMapper{}
}
