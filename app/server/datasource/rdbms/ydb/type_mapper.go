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

	isOptinal := regexp.MustCompile(`Optional<(\w+)>$`)

	for _, typeName := range typeNames {
		optional := false
		if matches := isOptinal.FindStringSubmatch(typeName); len(matches) > 0 {
			optional = true
			typeName = matches[1]
		}

		var (
			acceptor any
			appender func(acceptor any, builder array.Builder) error
			err      error
		)

		if !optional {
			acceptor, appender, err = makePrimitiveTransformers(typeName)
		} else {
			acceptor, appender, err = makeOptionalTransformers(typeName)
		}

		if err != nil {
			return nil, fmt.Errorf("make transformer: %w", err)
		}

		acceptors = append(acceptors, acceptor)
		appenders = append(appenders, appender)
	}

	return utils.NewRowTransformer[any](acceptors, appenders, nil), nil
}

func makePrimitiveTransformers(typeName string) (any, func(acceptor any, builder array.Builder) error, error) {
	switch typeName {
	case "Bool":
		return new(bool), appendValueToArrowBuilder[bool, uint8, *array.Uint8Builder, utils.BoolConverter], nil
	case "Int8":
		return new(int8), appendValueToArrowBuilder[int8, int8, *array.Int8Builder, utils.Int8Converter], nil
	case "Int16":
		return new(int16), appendValueToArrowBuilder[int16, int16, *array.Int16Builder, utils.Int16Converter], nil
	case "Int32":
		return new(int32), appendValueToArrowBuilder[int32, int32, *array.Int32Builder, utils.Int32Converter], nil
	case "Int64":
		return new(int64), appendValueToArrowBuilder[int64, int64, *array.Int64Builder, utils.Int64Converter], nil
	case "Uint8":
		return new(uint8), appendValueToArrowBuilder[uint8, uint8, *array.Uint8Builder, utils.Uint8Converter], nil
	case "Uint16":
		return new(uint16), appendValueToArrowBuilder[uint16, uint16, *array.Uint16Builder, utils.Uint16Converter], nil
	case "Uint32":
		return new(uint32), appendValueToArrowBuilder[uint32, uint32, *array.Uint32Builder, utils.Uint32Converter], nil
	case "Uint64":
		return new(uint64), appendValueToArrowBuilder[uint64, uint64, *array.Uint64Builder, utils.Uint64Converter], nil
	case "Float":
		return new(float32), appendValueToArrowBuilder[float32, float32, *array.Float32Builder, utils.Float32Converter], nil
	case "Double":
		return new(float64), appendValueToArrowBuilder[float64, float64, *array.Float64Builder, utils.Float64Converter], nil
	case "Utf8":
		return new(string), appendValueToArrowBuilder[string, []byte, *array.BinaryBuilder, utils.StringToBytesConverter], nil
	default:
		return nil, nil, fmt.Errorf("unknown primitive type '%v'", typeName)
	}
}

func makeOptionalTransformers(typeName string) (any, func(acceptor any, builder array.Builder) error, error) {
	switch typeName {
	case "Bool":
		return new(*bool), appendValueToArrowBuilder[bool, uint8, *array.Uint8Builder, utils.BoolConverter], nil
	case "Int8":
		return new(*int8), appendValueToArrowBuilder[int8, int8, *array.Int8Builder, utils.Int8Converter], nil
	case "Int16":
		return new(*int16), appendValueToArrowBuilder[int16, int16, *array.Int16Builder, utils.Int16Converter], nil
	case "Int32":
		return new(*int32), appendValueToArrowBuilder[int32, int32, *array.Int32Builder, utils.Int32Converter], nil
	case "Int64":
		return new(*int64), appendValueToArrowBuilder[int64, int64, *array.Int64Builder, utils.Int64Converter], nil
	case "Uint8":
		return new(*uint8), appendValueToArrowBuilder[uint8, uint8, *array.Uint8Builder, utils.Uint8Converter], nil
	case "Uint16":
		return new(*uint16), appendValueToArrowBuilder[uint16, uint16, *array.Uint16Builder, utils.Uint16Converter], nil
	case "Uint32":
		return new(*uint32), appendValueToArrowBuilder[uint32, uint32, *array.Uint32Builder, utils.Uint32Converter], nil
	case "Uint64":
		return new(*uint64), appendValueToArrowBuilder[uint64, uint64, *array.Uint64Builder, utils.Uint64Converter], nil
	case "Float":
		return new(*float32), appendValueToArrowBuilder[float32, float32, *array.Float32Builder, utils.Float32Converter], nil
	case "Double":
		return new(*float64), appendValueToArrowBuilder[float64, float64, *array.Float64Builder, utils.Float64Converter], nil
	case "Utf8":
		return new(*string), appendValueToArrowBuilder[string, []byte, *array.BinaryBuilder, utils.StringToBytesConverter], nil
	default:
		return nil, nil, fmt.Errorf("unknown primitive type '%v'", typeName)
	}
}

func NewTypeMapper() utils.TypeMapper {
	return typeMapper{
		isOptinal: regexp.MustCompile(`Optional<(\w+)>$`),
	}
}
