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
	typeBool      = "Bool"
	typeInt8      = "Int8"
	typeUint8     = "Uint8"
	typeInt16     = "Int16"
	typeUint16    = "Uint16"
	typeInt32     = "Int32"
	typeUint32    = "Uint32"
	typeInt64     = "Int64"
	typeUint64    = "Uint64"
	typeFloat     = "Float"
	typeDouble    = "Double"
	typeString    = "String"
	typeUtf8      = "Utf8"
	typeJSON      = "Json"
	typeDate      = "Date"
	typeDatetime  = "Datetime"
	typeTimestamp = "Timestamp"
)

func (typeMapper) SQLTypeToYDBColumn(columnName, typeName string, _rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
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

//nolint:gocyclo
func makePrimitiveTypeFromString(typeName string) (*Ydb.Type, error) {
	// TODO: add all types support
	// Reference table: https://ydb.yandex-team.ru/docs/yql/reference/types/
	switch typeName {
	case typeBool:
		return common.MakePrimitiveType(Ydb.Type_BOOL), nil
	case typeInt8:
		return common.MakePrimitiveType(Ydb.Type_INT8), nil
	case typeUint8:
		return common.MakePrimitiveType(Ydb.Type_UINT8), nil
	case typeInt16:
		return common.MakePrimitiveType(Ydb.Type_INT16), nil
	case typeUint16:
		return common.MakePrimitiveType(Ydb.Type_UINT16), nil
	case typeInt32:
		return common.MakePrimitiveType(Ydb.Type_INT32), nil
	case typeUint32:
		return common.MakePrimitiveType(Ydb.Type_UINT32), nil
	case typeInt64:
		return common.MakePrimitiveType(Ydb.Type_INT64), nil
	case typeUint64:
		return common.MakePrimitiveType(Ydb.Type_UINT64), nil
	case typeFloat:
		return common.MakePrimitiveType(Ydb.Type_FLOAT), nil
	case typeDouble:
		return common.MakePrimitiveType(Ydb.Type_DOUBLE), nil
	case typeString:
		return common.MakePrimitiveType(Ydb.Type_STRING), nil
	case typeUtf8:
		return common.MakePrimitiveType(Ydb.Type_UTF8), nil
	case typeJSON:
		return common.MakePrimitiveType(Ydb.Type_JSON), nil
	case typeDate:
		// YDB connector always returns date / time columns in YQL_FORMAT, because it is always fits YDB's date / time type value ranges
		return common.MakePrimitiveType(Ydb.Type_DATE), nil
	case typeDatetime:
		return common.MakePrimitiveType(Ydb.Type_DATETIME), nil
	case typeTimestamp:
		return common.MakePrimitiveType(Ydb.Type_TIMESTAMP), nil
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
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
	case typeBool:
		return new(*bool), appendToBuilderWithValuePtrConverter[bool, uint8, *array.Uint8Builder](cc.Bool()), nil
	case typeInt8:
		return new(*int8), appendToBuilderWithValuePtrConverter[int8, int8, *array.Int8Builder](cc.Int8()), nil
	case typeInt16:
		return new(*int16), appendToBuilderWithValuePtrConverter[int16, int16, *array.Int16Builder](cc.Int16()), nil
	case typeInt32:
		return new(*int32), appendToBuilderWithValuePtrConverter[int32, int32, *array.Int32Builder](cc.Int32()), nil
	case typeInt64:
		return new(*int64), appendToBuilderWithValuePtrConverter[int64, int64, *array.Int64Builder](cc.Int64()), nil
	case typeUint8:
		return new(*uint8), appendToBuilderWithValuePtrConverter[uint8, uint8, *array.Uint8Builder](cc.Uint8()), nil
	case typeUint16:
		return new(*uint16), appendToBuilderWithValuePtrConverter[uint16, uint16, *array.Uint16Builder](cc.Uint16()), nil
	case typeUint32:
		return new(*uint32), appendToBuilderWithValuePtrConverter[uint32, uint32, *array.Uint32Builder](cc.Uint32()), nil
	case typeUint64:
		return new(*uint64), appendToBuilderWithValuePtrConverter[uint64, uint64, *array.Uint64Builder](cc.Uint64()), nil
	case typeFloat:
		return new(*float32), appendToBuilderWithValuePtrConverter[float32, float32, *array.Float32Builder](cc.Float32()), nil
	case typeDouble:
		return new(*float64), appendToBuilderWithValuePtrConverter[float64, float64, *array.Float64Builder](cc.Float64()), nil
	case typeString:
		return new(*[]byte), appendToBuilderWithValuePtrConverter[[]byte, []byte, *array.BinaryBuilder](cc.Bytes()), nil
	case typeUtf8:
		return new(*string), appendToBuilderWithValuePtrConverter[string, string, *array.StringBuilder](cc.String()), nil
	case typeJSON:
		// Copy of UTF8
		return new(*string), appendToBuilderWithValuePtrConverter[string, string, *array.StringBuilder](cc.String()), nil
	case typeDate:
		switch ydbTypeID {
		case Ydb.Type_DATE:
			return new(*time.Time), appendToBuilderWithValuePtrConverter[time.Time, uint16, *array.Uint16Builder](cc.Date()), nil
		case Ydb.Type_UTF8:
			return new(*time.Time), appendToBuilderWithValuePtrConverter[time.Time, string, *array.StringBuilder](cc.DateToString()), nil
		default:
			return nil, nil,
				fmt.Errorf("unexpected ydb type id %v with sql type %s: %w", ydbTypeID, typeName, common.ErrDataTypeNotSupported)
		}
	case typeDatetime:
		switch ydbTypeID {
		case Ydb.Type_DATETIME:
			return new(*time.Time), appendToBuilderWithValuePtrConverter[time.Time, uint32, *array.Uint32Builder](cc.Datetime()), nil
		default:
			return nil, nil,
				fmt.Errorf("unexpected ydb type id %v with sql type %s: %w", ydbTypeID, typeName, common.ErrDataTypeNotSupported)
		}
	case typeTimestamp:
		switch ydbTypeID {
		case Ydb.Type_TIMESTAMP:
			return new(*time.Time), appendToBuilderWithValuePtrConverter[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()), nil
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
