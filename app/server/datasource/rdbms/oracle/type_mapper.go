package oracle

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
	isTimestamp     *regexp.Regexp
	isTimestampWTZ  *regexp.Regexp
	isTimestampWLTZ *regexp.Regexp
}

func (tm typeMapper) SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
	var (
		ydbType *Ydb.Type
		err     error
	)

	_ = rules

	// Oracle Data Types
	//	https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6
	// Reference table: https://github.com/ydb-platform/fq-connector-go/blob/main/docs/type_mapping_table.md
	switch {
	case typeName == "NUMBER":
		// TODO: NUMBER(p, s) can be float. Should convert to Decimal
		// 	Note: NUMBER can be from 1 to 22 bytes. Has wider range than Int64 or YDB Decimal. Possible representation - string
		//  		Possible optimisation: if p > 16 then to string, else to int64
		ydbType = common.MakePrimitiveType(Ydb.Type_INT64)
	// // go-ora
	// for some reason go-ora driver does not distinguish VARCHAR and NCHAR from time to time. go-ora valueTypes:
	// https://github.com/sijms/go-ora/blob/78d53fdf18c31d74e7fc9e0ebe49ee1c6af0abda/parameter.go#L30-L77
	case typeName == "NCHAR", typeName == "CHAR", typeName == "VARCHAR",
		typeName == "VARCHAR2", typeName == "NVARCHAR", typeName == "NVARCHAR2",
		typeName == "CLOB", typeName == "NCLOB", typeName == "LONG", typeName == "ROWID",
		typeName == "UROWID":
		ydbType = common.MakePrimitiveType(Ydb.Type_UTF8)
	case typeName == "RAW", typeName == "LONG RAW", typeName == "BLOB":
		ydbType = common.MakePrimitiveType(Ydb.Type_STRING)
	case typeName == "DATE":
		ydbType = common.MakePrimitiveType(Ydb.Type_DATETIME)
	case tm.isTimestamp.MatchString(typeName),
		tm.isTimestampWTZ.MatchString(typeName),
		tm.isTimestampWLTZ.MatchString(typeName):
		ydbType = common.MakePrimitiveType(Ydb.Type_TIMESTAMP)
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
	}

	if err != nil {
		return nil, fmt.Errorf("convert type '%s': %w", typeName, err)
	}

	// In Oracle all columns are actually nullable, hence we wrap every T in Optional<T>.
	ydbType = common.MakeOptionalType(ydbType)

	return &Ydb.Column{
		Name: columnName,
		Type: ydbType,
	}, nil
}

func transformerFromSQLTypes(types []string, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	_ = ydbTypes
	acceptors := make([]any, 0, len(types))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(types))

	// there is incostintance between table metadata query and go-ora driver.
	// for some reason driver renames some types to its own names.
	// "LONG RAW" -> "LongRaw"
	// "CLOB", "NCLOB" -> "LongVarChar"
	// "TIMESTAMP(*)" -> "TimeStampDTY"
	// "TIMESTAMP(*) WITH TIME ZONE" -> "TimeStampDTY"
	// "TIMESTAMP(*) WITH LOCAL TIME ZONE" -> "TimeStampLTZ_DTY"

	for i, typeName := range types {
		switch typeName {
		case "NUMBER":
			acceptors = append(acceptors, new(*int64))
			appenders = append(appenders, makeAppender[int64, int64, *array.Int64Builder](cc.Int64()))
		case "NCHAR", "CHAR", "LongVarChar", "LONG", "ROWID", "UROWID":
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, makeAppender[string, string, *array.StringBuilder](cc.String()))
		case "RAW", "LongRaw":
			acceptors = append(acceptors, new(*[]byte))
			appenders = append(appenders, makeAppender[[]byte, []byte, *array.BinaryBuilder](cc.Bytes()))
		case "DATE":
			// Oracle data types:
			// 	https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6
			// Oracle Date value range is much more wide than YDB's Datetime value range
			ydbType := ydbTypes[i]

			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				fmt.Printf("Setting string format\n")
				appenders = append(appenders,
					makeAppender[time.Time, string, *array.StringBuilder](cc.DatetimeToString()))
			case Ydb.Type_DATETIME:
				fmt.Printf("Setting YDB\n")
				appenders = append(appenders, makeAppender[time.Time, uint32, *array.Uint32Builder](cc.Datetime()))
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
			}
		case "TimeStampDTY", "TimeStampTZ_DTY", "TimeStampLTZ_DTY": // TIMESTAMP
			// Oracle data types:
			// 	https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6
			// Oracle Timestamp value range is much more wide than YDB's Timestamp value range, and/or more precise
			ydbType := ydbTypes[i]

			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				fmt.Printf("Setting string format\n")
				appenders = append(appenders,
					makeAppender[time.Time, string, *array.StringBuilder](cc.TimestampToString()))
			case Ydb.Type_TIMESTAMP:
				fmt.Printf("Setting YDB\n")
				appenders = append(appenders, makeAppender[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
			}
		default:
			return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
		}
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
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
	cast := acceptor.(**IN)

	if *cast == nil {
		builder.AppendNull()

		return nil
	}

	value := *cast

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

	return nil
}

func NewTypeMapper() datasource.TypeMapper {
	return typeMapper{
		isTimestamp:     regexp.MustCompile(`TIMESTAMP\((.+)\)`),
		isTimestampWTZ:  regexp.MustCompile(`TIMESTAMP\((.+)\) WITH TIME ZONE`),
		isTimestampWLTZ: regexp.MustCompile(`TIMESTAMP\((.+)\) WITH LOCAL TIME ZONE`),
	}
}
