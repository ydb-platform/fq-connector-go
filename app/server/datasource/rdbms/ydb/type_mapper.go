package ydb

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	api_service_protos "github.com/ydb-platform/fq-connector-go/libgo/service/protos"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

var _ utils.TypeMapper = typeMapper{}

type typeMapper struct {
	isOptinal *regexp.Regexp
}

func (tm typeMapper) SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
	var (
		ydbType *Ydb.Type
		err     error
	)

	optional := false
	if matches := tm.isOptinal.FindStringSubmatch(typeName); len(matches) > 0 {
		optional = true
		typeName = matches[1]
	}

	ydbType, err = makePrimitiveType(typeName)
	if err != nil {
		return nil, fmt.Errorf("make type: %w", err)
	}

	if optional {
		ydbType = makeOptionalType(ydbType)
	}

	return &Ydb.Column{Name: columnName, Type: ydbType}, nil
}

func makePrimitiveType(typeName string) (*Ydb.Type, error) {
	// TODO: add all types support
	// Reference table: https://ydb.yandex-team.ru/docs/yql/reference/types/
	switch {
	case typeName == "Bool":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}}, nil
	case typeName == "Int8":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT8}}, nil
	case typeName == "Uint8":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT8}}, nil
	case typeName == "Int16":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT16}}, nil
	case typeName == "Uint16":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT16}}, nil
	case typeName == "Int32":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}}, nil
	case typeName == "Uint32":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT32}}, nil
	case typeName == "Int64":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}}, nil
	case typeName == "Uint64":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}}, nil
	case typeName == "Float":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_FLOAT}}, nil
	case typeName == "Double":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE}}, nil
	case typeName == "String":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}}, nil
	case typeName == "Utf8":
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}}, nil
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, utils.ErrDataTypeNotSupported)
	}
}

func makeOptionalType(ydbType *Ydb.Type) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: ydbType}}}
}

func appendValueToArrowBuilder[IN utils.ValueType, OUT utils.ValueType, AB utils.ArrowBuilder[OUT], CONV utils.ValueConverter[IN, OUT]](
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
		if errors.Is(err, utils.ErrValueOutOfTypeBounds) {
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

func transformerFromSQLTypes(typeNames []string, ydbTypes []*Ydb.Type) (utils.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(typeNames))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(typeNames))

	for _, typeName := range typeNames {
		switch typeName {
		case "Bool", "Optional<Bool>":
			acceptors = append(acceptors, new(*bool))
			appenders = append(appenders, appendValueToArrowBuilder[bool, uint8, *array.Uint8Builder, utils.BoolConverter])
		case "Int8", "Optional<Int8>":
			acceptors = append(acceptors, new(*int8))
			appenders = append(appenders, appendValueToArrowBuilder[int8, int8, *array.Int8Builder, utils.Int8Converter])
		case "Int16", "Optional<Int16>":
			acceptors = append(acceptors, new(*int16))
			appenders = append(appenders, appendValueToArrowBuilder[int16, int16, *array.Int16Builder, utils.Int16Converter])
		case "Int32", "Optional<Int32>":
			acceptors = append(acceptors, new(*int32))
			appenders = append(appenders, appendValueToArrowBuilder[int32, int32, *array.Int32Builder, utils.Int32Converter])
		case "Int64", "Optional<Int64>":
			acceptors = append(acceptors, new(*int64))
			appenders = append(appenders, appendValueToArrowBuilder[int64, int64, *array.Int64Builder, utils.Int64Converter])
		case "Uint8", "Optional<Uint8>":
			acceptors = append(acceptors, new(*uint8))
			appenders = append(appenders, appendValueToArrowBuilder[uint8, uint8, *array.Uint8Builder, utils.Uint8Converter])
		case "Uint16", "Optional<Uint16>":
			acceptors = append(acceptors, new(*uint16))
			appenders = append(appenders, appendValueToArrowBuilder[uint16, uint16, *array.Uint16Builder, utils.Uint16Converter])
		case "Uint32", "Optional<Uint32>":
			acceptors = append(acceptors, new(*uint32))
			appenders = append(appenders, appendValueToArrowBuilder[uint32, uint32, *array.Uint32Builder, utils.Uint32Converter])
		case "Uint64", "Optional<Uint64>":
			acceptors = append(acceptors, new(*uint64))
			appenders = append(appenders, appendValueToArrowBuilder[uint64, uint64, *array.Uint64Builder, utils.Uint64Converter])
		case "Float", "Optional<Float>":
			acceptors = append(acceptors, new(*float32))
			appenders = append(appenders, appendValueToArrowBuilder[float32, float32, *array.Float32Builder, utils.Float32Converter])
		case "Double", "Optional<Double>":
			acceptors = append(acceptors, new(*float64))
			appenders = append(appenders, appendValueToArrowBuilder[float64, float64, *array.Float64Builder, utils.Float64Converter])
		case "Utf8", "Optional<Utf8>":
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, appendValueToArrowBuilder[string, []byte, *array.BinaryBuilder, utils.StringToBytesConverter])
		default:
			return nil, fmt.Errorf("unknown type '%v'", typeName)
		}
	}

	return utils.NewRowTransformer[any](acceptors, appenders, nil), nil
}

func NewTypeMapper() utils.TypeMapper {
	return typeMapper{
		isOptinal: regexp.MustCompile(`Optional<(\w+)>$`),
	}
}
