package clickhouse

import (
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
	switch { // JSON needs custom parser, has composite type name structure. Possible to parse into Arrow struct
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

func transformerFromSQLTypes(typeNames []string, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(typeNames))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(typeNames))

	tm := typeMapper{
		isFixedString: regexp.MustCompile(`FixedString\([0-9]+\)`),
		isDateTime:    regexp.MustCompile(`DateTime(\('[\w,/]+'\))?`),
		isDateTime64:  regexp.MustCompile(`DateTime64\(\d{1}(, '[\w,/]+')?\)`),
		isNullable:    regexp.MustCompile(`Nullable\((.+)\)`),
		isArray:       regexp.MustCompile(`Array\((.+)\)`),
	}

	var (
		nullable bool
		err      error
	)

	for i, typeName := range typeNames {
		nullable = false

		if matches := tm.isNullable.FindStringSubmatch(typeName); len(matches) > 0 {
			typeName = matches[1]
			nullable = true
		}

		if nullable {
			acceptors, appenders, err = addAcceptorAppenderFromSQLTypeNameNullable(typeName, ydbTypes[i], acceptors, appenders, cc, tm)
			if err != nil {
				return nil, fmt.Errorf("nullable: %w", err)
			}
		} else {
			acceptors, appenders, err = addAcceptorAppenderFromSQLTypeName(typeName, ydbTypes[i], acceptors, appenders, cc, tm)
			if err != nil {
				return nil, fmt.Errorf("nonnullable: %w", err)
			}
		}
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
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

func saturateDateTime(in *time.Time, minValue, maxValue time.Time) *time.Time {
	if in.Before(minValue) {
		*in = minValue
	}

	if in.After(maxValue) {
		*in = maxValue
	}

	return in
}

type dateToStringConverter struct {
	conv conversion.ValuePtrConverter[time.Time, string]
}

func (c dateToStringConverter) Convert(in *time.Time) (string, error) {
	return c.conv.Convert(saturateDateTime(in, minClickHouseDate, maxClickHouseDate))
}

type date32ToStringConverter struct {
	conv conversion.ValuePtrConverter[time.Time, string]
}

func (c date32ToStringConverter) Convert(in *time.Time) (string, error) {
	return c.conv.Convert(saturateDateTime(in, minClickHouseDate32, maxClickHouseDate32))
}

type dateTimeToStringConverter struct {
	conv conversion.ValuePtrConverter[time.Time, string]
}

func (c dateTimeToStringConverter) Convert(in *time.Time) (string, error) {
	return c.conv.Convert(saturateDateTime(in, minClickHouseDatetime, maxClickHouseDatetime))
}

type dateTime64ToStringConverter struct {
	conv conversion.ValuePtrConverter[time.Time, string]
}

func (c dateTime64ToStringConverter) Convert(in *time.Time) (string, error) {
	return c.conv.Convert(saturateDateTime(in, minClickHouseDatetime64, maxClickHouseDatetime64))
}

func NewTypeMapper() datasource.TypeMapper {
	return typeMapper{
		isFixedString: regexp.MustCompile(`FixedString\([0-9]+\)`),
		isDateTime:    regexp.MustCompile(`DateTime(\('[\w,/]+'\))?`),
		isDateTime64:  regexp.MustCompile(`DateTime64\(\d{1}(, '[\w,/]+')?\)`),
		isNullable:    regexp.MustCompile(`Nullable\((.+)\)`),
		isArray:       regexp.MustCompile(`Array\((.+)\)`),
	}
}
