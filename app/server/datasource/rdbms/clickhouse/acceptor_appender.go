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
func addAcceptorAppenderFromSQLTypeName(
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
		acceptors = append(acceptors, new(bool))
		appenders = append(appenders, makeAppender[bool, uint8, *array.Uint8Builder](cc.Bool()))
	case typeName == typeInt8:
		acceptors = append(acceptors, new(int8))
		appenders = append(appenders, makeAppender[int8, int8, *array.Int8Builder](cc.Int8()))
	case typeName == typeInt16:
		acceptors = append(acceptors, new(int16))
		appenders = append(appenders, makeAppender[int16, int16, *array.Int16Builder](cc.Int16()))
	case typeName == typeInt32:
		acceptors = append(acceptors, new(int32))
		appenders = append(appenders, makeAppender[int32, int32, *array.Int32Builder](cc.Int32()))
	case typeName == typeInt64:
		acceptors = append(acceptors, new(int64))
		appenders = append(appenders, makeAppender[int64, int64, *array.Int64Builder](cc.Int64()))
	case typeName == typeUInt8:
		acceptors = append(acceptors, new(uint8))
		appenders = append(appenders, makeAppender[uint8, uint8, *array.Uint8Builder](cc.Uint8()))
	case typeName == typeUInt16:
		acceptors = append(acceptors, new(uint16))
		appenders = append(appenders, makeAppender[uint16, uint16, *array.Uint16Builder](cc.Uint16()))
	case typeName == typeUInt32:
		acceptors = append(acceptors, new(uint32))
		appenders = append(appenders, makeAppender[uint32, uint32, *array.Uint32Builder](cc.Uint32()))
	case typeName == typeUInt64:
		acceptors = append(acceptors, new(uint64))
		appenders = append(appenders, makeAppender[uint64, uint64, *array.Uint64Builder](cc.Uint64()))
	case typeName == typeFloat32:
		acceptors = append(acceptors, new(float32))
		appenders = append(appenders, makeAppender[float32, float32, *array.Float32Builder](cc.Float32()))
	case typeName == typeFload64:
		acceptors = append(acceptors, new(float64))
		appenders = append(appenders, makeAppender[float64, float64, *array.Float64Builder](cc.Float64()))
	case typeName == typeString, tm.isFixedString.MatchString(typeName):
		// Looks like []byte would be a better option here, but clickhouse driver prefers string
		acceptors = append(acceptors, new(string))
		appenders = append(appenders, makeAppender[string, []byte, *array.BinaryBuilder](cc.StringToBytes()))
	case typeName == typeDate:
		acceptors = append(acceptors, new(time.Time))

		ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
		if err != nil {
			return nil, nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
		}

		switch ydbTypeID {
		case Ydb.Type_UTF8:
			appenders = append(appenders,
				makeAppender[time.Time, string, *array.StringBuilder](dateToStringConverter{conv: cc.DateToString()}))
		case Ydb.Type_DATE:
			appenders = append(appenders, makeAppender[time.Time, uint16, *array.Uint16Builder](cc.Date()))
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
		}
	case typeName == typeDate32:
		acceptors = append(acceptors, new(time.Time))

		ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
		if err != nil {
			return nil, nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
		}

		switch ydbTypeID {
		case Ydb.Type_UTF8:
			appenders = append(appenders,
				makeAppender[time.Time, string, *array.StringBuilder](date32ToStringConverter{conv: cc.DateToString()}))
		case Ydb.Type_DATE:
			appenders = append(appenders, makeAppender[time.Time, uint16, *array.Uint16Builder](cc.Date()))
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
		}
	case tm.isDateTime64.MatchString(typeName):
		acceptors = append(acceptors, new(time.Time))

		ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
		if err != nil {
			return nil, nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
		}

		switch ydbTypeID {
		case Ydb.Type_UTF8:
			appenders = append(appenders,
				makeAppender[time.Time, string, *array.StringBuilder](dateTime64ToStringConverter{conv: cc.TimestampToString()}))
		case Ydb.Type_TIMESTAMP:
			appenders = append(appenders, makeAppender[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
		}
	case tm.isDateTime.MatchString(typeName):
		acceptors = append(acceptors, new(time.Time))

		ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
		if err != nil {
			return nil, nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
		}

		switch ydbTypeID {
		case Ydb.Type_UTF8:
			appenders = append(appenders,
				makeAppender[time.Time, string, *array.StringBuilder](dateTimeToStringConverter{conv: cc.DatetimeToString()}))
		case Ydb.Type_DATETIME:
			appenders = append(appenders, makeAppender[time.Time, uint32, *array.Uint32Builder](cc.Datetime()))
		default:
			return nil, nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
		}
	default:
		return nil, nil, fmt.Errorf("unknown type '%v'", typeName)
	}

	return acceptors, appenders, nil
}

func makeAppender[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](conv conversion.ValuePtrConverter[IN, OUT]) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		return appendValueToArrowBuilder[IN, OUT, AB](acceptor, builder, conv)
	}
}

func appendValueToArrowBuilder[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT]](
	acceptor any,
	builder array.Builder,
	conv conversion.ValuePtrConverter[IN, OUT],
) error {
	cast := acceptor.(*IN)

	if cast == nil {
		builder.AppendNull()

		return nil
	}

	out, err := conv.Convert(cast)
	if err != nil {
		if errors.Is(err, common.ErrValueOutOfTypeBounds) {
			// TODO: write warning to logger
			builder.AppendNull()

			return nil
		}

		return fmt.Errorf("convert value %v: %w", *cast, err)
	}

	//nolint:forcetypeassert
	builder.(AB).Append(out)

	return nil
}
