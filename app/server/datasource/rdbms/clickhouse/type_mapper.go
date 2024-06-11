package clickhouse

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
	isFixedString *regexp.Regexp
	isDateTime    *regexp.Regexp
	isDateTime64  *regexp.Regexp
	isNullable    *regexp.Regexp
	isArray       *regexp.Regexp
}

//nolint:gocyclo
func (tm typeMapper) SQLTypeToYDBColumn(
	columnName, typeName string,
	rules *api_service_protos.TTypeMappingSettings,
) (*Ydb.Column, error) {
	var (
		ydbType *Ydb.Type
		err     error
	)

	// By default all columns in CH are non-nullable, so
	// we wrap YDB types into Optional type only in such cases:
	//
	// 1. The column is explicitly defined as nullable;
	// 2. The column type is a date/time. CH value ranges for date/time are much wider than YQL value ranges,
	// so every time we encounter a value that is out of YQL ranges, we have to return NULL.
	nullable := false
	arrayContainer := false
	innerNullable := false

	if matches := tm.isNullable.FindStringSubmatch(typeName); len(matches) > 0 {
		nullable = true
		typeName = matches[1]
	}

	if matches := tm.isArray.FindStringSubmatch(typeName); len(matches) > 0 {
		arrayContainer = true
		typeName = matches[1]

		if matches := tm.isNullable.FindStringSubmatch(typeName); len(matches) > 0 {
			innerNullable = true
			typeName = matches[1]
		}
	}

	if arrayContainer {
		if nullable {
			return nil, fmt.Errorf("convert type '%s' (nullable array is not supported): %w",
				typeName, common.ErrDataTypeNotSupported)
		} else if innerNullable {
			return nil, fmt.Errorf("convert type '%s' (array with nullable elements is not supported): %w",
				typeName, common.ErrDataTypeNotSupported)
		}

		return nil, fmt.Errorf("convert type '%s' (array is not supported): %w",
			typeName, common.ErrDataTypeNotSupported)
	}

	// Reference table: https://github.com/ydb-platform/fq-connector-go/blob/main/docs/type_mapping_table.md
	switch {
	case typeName == "Bool":
		ydbType = common.MakePrimitiveType(Ydb.Type_BOOL)
	case typeName == "Int8":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT8)
	case typeName == "UInt8":
		ydbType = common.MakePrimitiveType(Ydb.Type_UINT8)
	case typeName == "Int16":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT16)
	case typeName == "UInt16":
		ydbType = common.MakePrimitiveType(Ydb.Type_UINT16)
	case typeName == "Int32":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT32)
	case typeName == "UInt32":
		ydbType = common.MakePrimitiveType(Ydb.Type_UINT32)
	case typeName == "Int64":
		ydbType = common.MakePrimitiveType(Ydb.Type_INT64)
	case typeName == "UInt64":
		ydbType = common.MakePrimitiveType(Ydb.Type_UINT64)
	case typeName == "Float32":
		ydbType = common.MakePrimitiveType(Ydb.Type_FLOAT)
	case typeName == "Float64":
		ydbType = common.MakePrimitiveType(Ydb.Type_DOUBLE)
	// String/FixedString are binary in ClickHouse, so we map it to YDB's String instead of UTF8:
	// https://ydb.tech/en/docs/yql/reference/types/primitive#string
	// https://clickhouse.com/docs/en/sql-reference/data-types/string#encodings
	case typeName == "String", tm.isFixedString.MatchString(typeName):
		ydbType = common.MakePrimitiveType(Ydb.Type_STRING)
	case typeName == "Date", typeName == "Date32":
		// NOTE: ClickHouse's Date32 value range is much more wide than YDB's Date value range
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_DATE, rules.GetDateTimeFormat())
		nullable = nullable || rules.GetDateTimeFormat() == api_service_protos.EDateTimeFormat_YQL_FORMAT
	case tm.isDateTime64.MatchString(typeName):
		// NOTE: ClickHouse's DateTime64 value range is much more wide than YDB's Timestamp value range
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_TIMESTAMP, rules.GetDateTimeFormat())
		nullable = nullable || rules.GetDateTimeFormat() == api_service_protos.EDateTimeFormat_YQL_FORMAT
	case tm.isDateTime.MatchString(typeName):
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_DATETIME, rules.GetDateTimeFormat())
		nullable = nullable || rules.GetDateTimeFormat() == api_service_protos.EDateTimeFormat_YQL_FORMAT
	default:
		err = fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
	}

	if err != nil {
		return nil, err
	}

	// If the column is nullable, wrap it into YQL's optional
	if nullable {
		ydbType = common.MakeOptionalType(ydbType)
	}

	return &Ydb.Column{
		Name: columnName,
		Type: ydbType,
	}, nil
}

//nolint:funlen,gocyclo
func transformerFromSQLTypes(typeNames []string, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(typeNames))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(typeNames))
	isNullable := regexp.MustCompile(`Nullable\((?P<Internal>[\w\(\)]+)\)`)
	isFixedString := regexp.MustCompile(`FixedString\([0-9]+\)`)
	isDateTime := regexp.MustCompile(`DateTime(\('[\w,/]+'\))?`)
	isDateTime64 := regexp.MustCompile(`DateTime64\(\d{1}(, '[\w,/]+')?\)`)

	for i, typeName := range typeNames {
		if matches := isNullable.FindStringSubmatch(typeName); len(matches) > 0 {
			typeName = matches[1]
		}

		switch {
		case typeName == "Bool":
			acceptors = append(acceptors, new(*bool))
			appenders = append(appenders, makeAppender[bool, uint8, *array.Uint8Builder](cc.Bool()))
		case typeName == "Int8":
			acceptors = append(acceptors, new(*int8))
			appenders = append(appenders, makeAppender[int8, int8, *array.Int8Builder](cc.Int8()))
		case typeName == "Int16":
			acceptors = append(acceptors, new(*int16))
			appenders = append(appenders, makeAppender[int16, int16, *array.Int16Builder](cc.Int16()))
		case typeName == "Int32":
			acceptors = append(acceptors, new(*int32))
			appenders = append(appenders, makeAppender[int32, int32, *array.Int32Builder](cc.Int32()))
		case typeName == "Int64":
			acceptors = append(acceptors, new(*int64))
			appenders = append(appenders, makeAppender[int64, int64, *array.Int64Builder](cc.Int64()))
		case typeName == "UInt8":
			acceptors = append(acceptors, new(*uint8))
			appenders = append(appenders, makeAppender[uint8, uint8, *array.Uint8Builder](cc.Uint8()))
		case typeName == "UInt16":
			acceptors = append(acceptors, new(*uint16))
			appenders = append(appenders, makeAppender[uint16, uint16, *array.Uint16Builder](cc.Uint16()))
		case typeName == "UInt32":
			acceptors = append(acceptors, new(*uint32))
			appenders = append(appenders, makeAppender[uint32, uint32, *array.Uint32Builder](cc.Uint32()))
		case typeName == "UInt64":
			acceptors = append(acceptors, new(*uint64))
			appenders = append(appenders, makeAppender[uint64, uint64, *array.Uint64Builder](cc.Uint64()))
		case typeName == "Float32":
			acceptors = append(acceptors, new(*float32))
			appenders = append(appenders, makeAppender[float32, float32, *array.Float32Builder](cc.Float32()))
		case typeName == "Float64":
			acceptors = append(acceptors, new(*float64))
			appenders = append(appenders, makeAppender[float64, float64, *array.Float64Builder](cc.Float64()))
		case typeName == "String", isFixedString.MatchString(typeName):
			// Looks like []byte would be a better option here, but clickhouse driver prefers string
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, makeAppender[string, []byte, *array.BinaryBuilder](cc.StringToBytes()))
		case typeName == "Date":
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders,
					makeAppender[time.Time, string, *array.StringBuilder](dateToStringConverter{conv: cc.DateToString()}))
			case Ydb.Type_DATE:
				appenders = append(appenders, makeAppender[time.Time, uint16, *array.Uint16Builder](cc.Date()))
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbTypes[i], typeName, common.ErrDataTypeNotSupported)
			}
		case typeName == "Date32":
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders,
					makeAppender[time.Time, string, *array.StringBuilder](date32ToStringConverter{conv: cc.DateToString()}))
			case Ydb.Type_DATE:
				appenders = append(appenders, makeAppender[time.Time, uint16, *array.Uint16Builder](cc.Date()))
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbTypes[i], typeName, common.ErrDataTypeNotSupported)
			}
		case isDateTime64.MatchString(typeName):
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders,
					makeAppender[time.Time, string, *array.StringBuilder](dateTime64ToStringConverter{conv: cc.TimestampToString()}))
			case Ydb.Type_TIMESTAMP:
				appenders = append(appenders, makeAppender[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbTypes[i], typeName, common.ErrDataTypeNotSupported)
			}
		case isDateTime.MatchString(typeName):
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders,
					makeAppender[time.Time, string, *array.StringBuilder](dateTimeToStringConverter{conv: cc.DatetimeToString()}))
			case Ydb.Type_DATETIME:
				appenders = append(appenders, makeAppender[time.Time, uint32, *array.Uint32Builder](cc.Datetime()))
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbTypes[i], typeName, common.ErrDataTypeNotSupported)
			}
		default:
			return nil, fmt.Errorf("unknown type '%v'", typeName)
		}
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
}

func makeAppender[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](conv conversion.ValueConverter[IN, OUT]) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		return appendValueToArrowBuilder[IN, OUT, AB](acceptor, builder, conv)
	}
}

func appendValueToArrowBuilder[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT]](
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

// If time value is under of type bounds ClickHouse behavior is undefined
// See note: https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#tostartofmonth

var (
	minClickHouseDate       = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxClickHouseDate       = time.Date(2149, time.June, 6, 0, 0, 0, 0, time.UTC)
	minClickHouseDate32     = time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxClickHouseDate32     = time.Date(2299, time.December, 31, 0, 0, 0, 0, time.UTC)
	minClickHouseDatetime   = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxClickHouseDatetime   = time.Date(2106, time.February, 7, 6, 28, 15, 0, time.UTC)
	minClickHouseDatetime64 = time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxClickHouseDatetime64 = time.Date(2299, time.December, 31, 23, 59, 59, 99999999, time.UTC)
)

func saturateDateTime(in, min, max time.Time) *time.Time {
	if in.Before(min) {
		in = min
	}

	if in.After(max) {
		in = max
	}

	return &in
}

type dateToStringConverter struct {
	conv conversion.ValuePtrConverter[time.Time, string]
}

func (c dateToStringConverter) Convert(in time.Time) (string, error) {
	return c.conv.Convert(saturateDateTime(in, minClickHouseDate, maxClickHouseDate))
}

type date32ToStringConverter struct {
	conv conversion.ValuePtrConverter[time.Time, string]
}

func (c date32ToStringConverter) Convert(in time.Time) (string, error) {
	return c.conv.Convert(saturateDateTime(in, minClickHouseDate32, maxClickHouseDate32))
}

type dateTimeToStringConverter struct {
	conv conversion.ValuePtrConverter[time.Time, string]
}

func (c dateTimeToStringConverter) Convert(in time.Time) (string, error) {
	return c.conv.Convert(saturateDateTime(in, minClickHouseDatetime, maxClickHouseDatetime))
}

type dateTime64ToStringConverter struct {
	conv conversion.ValuePtrConverter[time.Time, string]
}

func (c dateTime64ToStringConverter) Convert(in time.Time) (string, error) {
	return c.conv.Convert(saturateDateTime(in, minClickHouseDatetime64, maxClickHouseDatetime64))
}

func NewTypeMapper() datasource.TypeMapper {
	return typeMapper{
		isFixedString: regexp.MustCompile(`FixedString\([0-9]+\)`),
		isDateTime:    regexp.MustCompile(`DateTime(\('[\w,/]+'\))?`),
		isDateTime64:  regexp.MustCompile(`DateTime64\(\d{1}(, '[\w,/]+')?\)`),
		isNullable:    regexp.MustCompile(`Nullable\((?P<Internal>[\w\(\)]+)\)`),
		isArray:       regexp.MustCompile(`Array\((?P<Internal>[\w\(\)]+)\)`),
	}
}
