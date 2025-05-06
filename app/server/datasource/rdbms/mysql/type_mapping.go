package mysql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.TypeMapper = &typeMapper{}

type typeMapper struct {
	reType *regexp.Regexp
}

//nolint:gocyclo
func (tm *typeMapper) SQLTypeToYDBColumn(
	columnName, columnType string,
	typeMapperSettings *api_service_protos.TTypeMappingSettings,
) (*Ydb.Column, error) {
	var (
		typeName string
		typeSize uint64
		err      error
	)

	typeNameWithoutModifier := strings.Split(columnType, " ")[0]

	if matches := tm.reType.FindStringSubmatch(columnType); len(matches) > 0 {
		typeName = matches[tm.reType.SubexpIndex("type")]
		typeSize, err = strconv.ParseUint(matches[tm.reType.SubexpIndex("size")], 10, 64)

		if err != nil {
			return nil, fmt.Errorf("parse uint: %w", err)
		}
	} else {
		typeName = typeNameWithoutModifier
	}

	unsigned := strings.Contains(columnType, "unsigned")

	ydbColumn := Ydb.Column{Name: columnName}

	switch typeName {
	case typeInt, typeMediumInt:
		var ydbType Ydb.Type_PrimitiveTypeId

		if unsigned {
			ydbType = Ydb.Type_UINT32
		} else {
			ydbType = Ydb.Type_INT32
		}

		ydbColumn.Type = common.MakePrimitiveType(ydbType)
	case typeBigInt:
		var ydbType Ydb.Type_PrimitiveTypeId

		if unsigned {
			ydbType = Ydb.Type_UINT64
		} else {
			ydbType = Ydb.Type_INT64
		}

		ydbColumn.Type = common.MakePrimitiveType(ydbType)
	case typeFloat:
		ydbColumn.Type = common.MakePrimitiveType(Ydb.Type_FLOAT)
	case typeDouble:
		ydbColumn.Type = common.MakePrimitiveType(Ydb.Type_DOUBLE)
	case typeTinyInt:
		if typeSize == 1 {
			ydbColumn.Type = common.MakePrimitiveType(Ydb.Type_BOOL)
		} else if unsigned {
			ydbColumn.Type = common.MakePrimitiveType(Ydb.Type_UINT8)
		} else {
			ydbColumn.Type = common.MakePrimitiveType(Ydb.Type_INT8)
		}
	case typeSmallInt:
		var ydbType Ydb.Type_PrimitiveTypeId

		if unsigned {
			ydbType = Ydb.Type_UINT16
		} else {
			ydbType = Ydb.Type_INT16
		}

		ydbColumn.Type = common.MakePrimitiveType(ydbType)
	case typeLongBlob, typeBlob, typeMediumBlob, typeTinyBlob:
		ydbColumn.Type = common.MakePrimitiveType(Ydb.Type_STRING)
	case typeVarChar, typeChar:
		ydbColumn.Type = common.MakePrimitiveType(Ydb.Type_UTF8)
	case typeBinary, typeVarBinary:
		ydbColumn.Type = common.MakePrimitiveType(Ydb.Type_STRING)
	case typeText, typeLongText, typeTinyText, typeMediumText:
		ydbColumn.Type = common.MakePrimitiveType(Ydb.Type_STRING)
	case typeDate:
		ydbColumn.Type, err = common.MakeYdbDateTimeType(Ydb.Type_DATE, typeMapperSettings.GetDateTimeFormat())
		if err != nil {
			return nil, fmt.Errorf("make YDB date/time type: %w", err)
		}
	// case "time":
	// TODO: YDB has no separate type representing time of the day
	case typeDatetime, typeTimestamp:
		// In MySQL `Datetime` and `Timestamp` are quite similar.
		// Both of them can store fractional seconds (up to 6 digits).
		//
		// Here are the key differences:
		// 1. Value range:
		// 		- `Datetime`: '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999' (8 bytes)
		// 		- `Timestamp` range: '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.499999' (4 bytes)
		// 2. Timezone awareness: see https://dev.mysql.com/doc/refman/8.4/en/datetime.html for details
		//
		// So we map both into YDB's Timestamp.
		ydbColumn.Type, err = common.MakeYdbDateTimeType(Ydb.Type_TIMESTAMP, typeMapperSettings.GetDateTimeFormat())
		if err != nil {
			return nil, fmt.Errorf("make YDB date/time type: %w", err)
		}
	case typeJSON:
		ydbColumn.Type = common.MakePrimitiveType(Ydb.Type_JSON)
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
	}

	ydbColumn.Type = common.MakeOptionalType(ydbColumn.GetType())

	return &ydbColumn, nil
}

func NewTypeMapper() datasource.TypeMapper {
	return &typeMapper{
		regexp.MustCompile(`(?P<type>.*)(:?\((?P<size>\d+)\))`),
	}
}

func transformerFromSQLTypes(
	mySQLTypes []uint8,
	ydbTypes []*Ydb.Type,
	cc conversion.Collection,
) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ydbTypes))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(ydbTypes))

	for i := range mySQLTypes {
		if err := addAcceptorAppender(mySQLTypes[i], ydbTypes[i], cc, &acceptors, &appenders); err != nil {
			return nil, fmt.Errorf("add acceptor appender #%d: %w", i, err)
		}
	}

	return paging.NewRowTransformer(acceptors, appenders, nil), nil
}

//nolint:funlen,gocyclo
func addAcceptorAppender(
	mySQLType uint8,
	ydbType *Ydb.Type,
	cc conversion.Collection,
	acceptors *[]any,
	appenders *[]func(acceptor any, builder array.Builder) error,
) error {
	ydbTypeId, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
	if err != nil {
		return fmt.Errorf("ydb type to ydb primitive type id: %w", err)
	}

	switch mySQLType {
	case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
		switch ydbTypeId {
		case Ydb.Type_UINT32:
			*acceptors = append(*acceptors, new(*uint32))
			*appenders = append(*appenders, utils.MakeAppenderNullable[uint32, uint32, *array.Uint32Builder](cc.Uint32()))
		case Ydb.Type_INT32:
			*acceptors = append(*acceptors, new(*int32))
			*appenders = append(*appenders, utils.MakeAppenderNullable[int32, int32, *array.Int32Builder](cc.Int32()))
		default:
			return fmt.Errorf("type mismatch: mysql '%d' vs ydb '%s': %w", mySQLType, ydbTypeId.String(), common.ErrDataTypeNotSupported)
		}
	case mysql.MYSQL_TYPE_LONGLONG:
		switch ydbTypeId {
		case Ydb.Type_UINT64:
			*acceptors = append(*acceptors, new(*uint64))
			*appenders = append(*appenders, utils.MakeAppenderNullable[uint64, uint64, *array.Uint64Builder](cc.Uint64()))
		case Ydb.Type_INT64:
			*acceptors = append(*acceptors, new(*int64))
			*appenders = append(*appenders, utils.MakeAppenderNullable[int64, int64, *array.Int64Builder](cc.Int64()))
		default:
			return fmt.Errorf("type mismatch: mysql '%d' vs ydb '%s': %w", mySQLType, ydbTypeId.String(), common.ErrDataTypeNotSupported)
		}
	case mysql.MYSQL_TYPE_FLOAT:
		*acceptors = append(*acceptors, new(*float32))
		*appenders = append(*appenders, utils.MakeAppenderNullable[float32, float32, *array.Float32Builder](cc.Float32()))
	case mysql.MYSQL_TYPE_DOUBLE:
		*acceptors = append(*acceptors, new(*float64))
		*appenders = append(*appenders, utils.MakeAppenderNullable[float64, float64, *array.Float64Builder](cc.Float64()))
	case mysql.MYSQL_TYPE_TINY:
		switch ydbTypeId {
		case Ydb.Type_BOOL:
			*acceptors = append(*acceptors, new(*bool))
			*appenders = append(*appenders, utils.MakeAppenderNullable[bool, uint8, *array.Uint8Builder](cc.Bool()))
		case Ydb.Type_UINT8:
			*acceptors = append(*acceptors, new(*uint8))
			*appenders = append(*appenders, utils.MakeAppenderNullable[uint8, uint8, *array.Uint8Builder](cc.Uint8()))
		case Ydb.Type_INT8:
			*acceptors = append(*acceptors, new(*int8))
			*appenders = append(*appenders, utils.MakeAppenderNullable[int8, int8, *array.Int8Builder](cc.Int8()))
		default:
			return fmt.Errorf("type mismatch: mysql '%d' vs ydb '%s': %w", mySQLType, ydbTypeId.String(), common.ErrDataTypeNotSupported)
		}
	case mysql.MYSQL_TYPE_SHORT:
		switch ydbTypeId {
		case Ydb.Type_UINT16:
			*acceptors = append(*acceptors, new(*uint16))
			*appenders = append(*appenders, utils.MakeAppenderNullable[uint16, uint16, *array.Uint16Builder](cc.Uint16()))
		case Ydb.Type_INT16:
			*acceptors = append(*acceptors, new(*int16))
			*appenders = append(*appenders, utils.MakeAppenderNullable[int16, int16, *array.Int16Builder](cc.Int16()))
		default:
			return fmt.Errorf("type mismatch: mysql '%d' vs ydb '%s': %w", mySQLType, ydbTypeId.String(), common.ErrDataTypeNotSupported)
		}
	case mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_TINY_BLOB:
		*acceptors = append(*acceptors, new(*[]byte))
		*appenders = append(*appenders, utils.MakeAppenderNullable[[]byte, []byte, *array.BinaryBuilder](cc.Bytes()))
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VAR_STRING:
		*acceptors = append(*acceptors, new(*string))

		switch ydbTypeId {
		case Ydb.Type_UTF8:
			*appenders = append(*appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		case Ydb.Type_STRING:
			*appenders = append(*appenders, utils.MakeAppenderNullable[string, []byte, *array.BinaryBuilder](cc.StringToBytes()))
		default:
			return fmt.Errorf("type mismatch: mysql '%d' vs ydb '%s': %w", mySQLType, ydbTypeId.String(), common.ErrDataTypeNotSupported)
		}
	case mysql.MYSQL_TYPE_DATE:
		*acceptors = append(*acceptors, new(*time.Time))

		switch ydbTypeId {
		case Ydb.Type_UTF8:
			*appenders = append(*appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(**time.Time)

				return utils.MakeAppenderNullable[time.Time, string, *array.StringBuilder](cc.DateToString())(cast, builder)
			})
		case Ydb.Type_DATE:
			*appenders = append(*appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(**time.Time)

				return utils.MakeAppenderNullable[time.Time, uint16, *array.Uint16Builder](cc.Date())(cast, builder)
			})
		default:
			return fmt.Errorf("type mismatch: mysql '%d' vs ydb '%s': %w", mySQLType, ydbTypeId.String(), common.ErrDataTypeNotSupported)
		}
	// case "time":
	// TODO: YDB has no separate type representing time of the day
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2, mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
		*acceptors = append(*acceptors, new(*time.Time))

		switch ydbTypeId {
		case Ydb.Type_UTF8:
			*appenders = append(*appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(**time.Time)

				// YQ-3608: only TIMESTAMP type contain zonal information,
				// therefore, we can append 'Z' suffix only to TIMESTAMP column (not DATETIME)
				utc := mySQLType == mysql.MYSQL_TYPE_TIMESTAMP || mySQLType == mysql.MYSQL_TYPE_TIMESTAMP2

				return utils.MakeAppenderNullable[time.Time, string, *array.StringBuilder](cc.TimestampToString(utc))(cast, builder)
			})
		case Ydb.Type_TIMESTAMP:
			*appenders = append(*appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(**time.Time)

				return utils.MakeAppenderNullable[time.Time, uint64, *array.Uint64Builder](cc.Timestamp())(cast, builder)
			})
		default:
			return fmt.Errorf("type mismatch: mysql '%d' vs ydb '%s': %w", mySQLType, ydbTypeId.String(), common.ErrDataTypeNotSupported)
		}
	case mysql.MYSQL_TYPE_JSON:
		*acceptors = append(*acceptors, new(*string))
		*appenders = append(*appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
	default:
		return fmt.Errorf("unexpected mysql type '%d': %w", mySQLType, common.ErrDataTypeNotSupported)
	}

	return nil
}
