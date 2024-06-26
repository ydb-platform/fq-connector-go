package clickhouse

import (
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/common"
)

//nolint:funlen,gocyclo
func addAcceptorAppenderFromSQLTypeNameNullable(
	typeName string,
	ydbType *Ydb.Type,
	acceptors []any,
	appenders []func(acceptor any, builder array.Builder) error,
	cc conversion.Collection,
	tm typeMapper,
) (
	[]any,
	[]func(acceptor any, builder array.Builder) error,
	error,
) {
	switch {
	case typeName == typeBool:
		acceptors = append(acceptors, new(*bool))
		appenders = append(appenders, makeAppenderNullable[bool, uint8, *array.Uint8Builder](cc.Bool()))
	case typeName == typeInt8:
		acceptors = append(acceptors, new(*int8))
		appenders = append(appenders, makeAppenderNullable[int8, int8, *array.Int8Builder](cc.Int8()))
	case typeName == typeInt16:
		acceptors = append(acceptors, new(*int16))
		appenders = append(appenders, makeAppenderNullable[int16, int16, *array.Int16Builder](cc.Int16()))
	case typeName == typeInt32:
		acceptors = append(acceptors, new(*int32))
		appenders = append(appenders, makeAppenderNullable[int32, int32, *array.Int32Builder](cc.Int32()))
	case typeName == typeInt64:
		acceptors = append(acceptors, new(*int64))
		appenders = append(appenders, makeAppenderNullable[int64, int64, *array.Int64Builder](cc.Int64()))
	case typeName == typeUInt8:
		acceptors = append(acceptors, new(*uint8))
		appenders = append(appenders, makeAppenderNullable[uint8, uint8, *array.Uint8Builder](cc.Uint8()))
	case typeName == typeUInt16:
		acceptors = append(acceptors, new(*uint16))
		appenders = append(appenders, makeAppenderNullable[uint16, uint16, *array.Uint16Builder](cc.Uint16()))
	case typeName == typeUInt32:
		acceptors = append(acceptors, new(*uint32))
		appenders = append(appenders, makeAppenderNullable[uint32, uint32, *array.Uint32Builder](cc.Uint32()))
	case typeName == typeUInt64:
		acceptors = append(acceptors, new(*uint64))
		appenders = append(appenders, makeAppenderNullable[uint64, uint64, *array.Uint64Builder](cc.Uint64()))
	case typeName == typeFloat32:
		acceptors = append(acceptors, new(*float32))
		appenders = append(appenders, makeAppenderNullable[float32, float32, *array.Float32Builder](cc.Float32()))
	case typeName == typeFload64:
		acceptors = append(acceptors, new(*float64))
		appenders = append(appenders, makeAppenderNullable[float64, float64, *array.Float64Builder](cc.Float64()))
	case typeName == typeString, tm.isFixedString.MatchString(typeName):
		// Looks like []byte would be a better option here, but clickhouse driver prefers string
		acceptors = append(acceptors, new(*string))
		appenders = append(appenders, makeAppenderNullable[string, []byte, *array.BinaryBuilder](cc.StringToBytes()))
	case typeName == typeDate:
		acceptors = append(acceptors, new(*time.Time))

		ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
		if err != nil {
			return nil, nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
		}

		switch ydbTypeID {
		case Ydb.Type_UTF8:
			appenders = append(appenders,
				makeAppenderNullable[time.Time, string, *array.StringBuilder](dateToStringConverter{conv: cc.DateToString()}))
		case Ydb.Type_DATE:
			appenders = append(appenders, makeAppenderNullable[time.Time, uint16, *array.Uint16Builder](cc.Date()))
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
		}
	case typeName == typeDate32:
		acceptors = append(acceptors, new(*time.Time))

		ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
		if err != nil {
			return nil, nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
		}

		switch ydbTypeID {
		case Ydb.Type_UTF8:
			appenders = append(appenders,
				makeAppenderNullable[time.Time, string, *array.StringBuilder](date32ToStringConverter{conv: cc.DateToString()}))
		case Ydb.Type_DATE:
			appenders = append(appenders, makeAppenderNullable[time.Time, uint16, *array.Uint16Builder](cc.Date()))
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
		}
	case tm.isDateTime64.MatchString(typeName):
		acceptors = append(acceptors, new(*time.Time))

		ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
		if err != nil {
			return nil, nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
		}

		switch ydbTypeID {
		case Ydb.Type_UTF8:
			appenders = append(appenders,
				makeAppenderNullable[time.Time, string, *array.StringBuilder](dateTime64ToStringConverter{conv: cc.TimestampToString()}))
		case Ydb.Type_TIMESTAMP:
			appenders = append(appenders, makeAppenderNullable[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
		}
	case tm.isDateTime.MatchString(typeName):
		acceptors = append(acceptors, new(*time.Time))

		ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
		if err != nil {
			return nil, nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
		}

		switch ydbTypeID {
		case Ydb.Type_UTF8:
			appenders = append(appenders,
				makeAppenderNullable[time.Time, string, *array.StringBuilder](dateTimeToStringConverter{conv: cc.DatetimeToString()}))
		case Ydb.Type_DATETIME:
			appenders = append(appenders, makeAppenderNullable[time.Time, uint32, *array.Uint32Builder](cc.Datetime()))
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
		}
	default:
		return nil, nil, fmt.Errorf("unknown type '%v'", typeName)
	}

	return acceptors, appenders, nil
}

func makeAppenderNullable[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](conv conversion.ValueConverter[IN, OUT]) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		return appendValueToArrowBuilderNullable[IN, OUT, AB](acceptor, builder, conv)
	}
}

func appendValueToArrowBuilderNullable[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT]](
	acceptor any,
	builder array.Builder,
	conv conversion.ValueConverter[IN, OUT],
) error {
	cast := acceptor.(**IN)

	if *cast == nil {
		builder.AppendNull()

		return nil
	}

	value := **cast

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

	// Without that ClickHouse native driver would return invalid values for NULLABLE(bool) columns;
	// TODO: research it.
	*cast = nil

	return nil
}
