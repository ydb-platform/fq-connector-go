package oracle

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
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
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
	// YQ-3498: go-ora driver has a bug when reading BINARY_FLOAT -1.1, gives -1.2
	// case typeName == "BINARY_FLOAT":
	// 	ydbType = common.MakePrimitiveType(Ydb.Type_FLOAT) // driver giver float64 in driver.Value
	case typeName == "BINARY_DOUBLE":
		ydbType = common.MakePrimitiveType(Ydb.Type_DOUBLE)
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
	case typeName == "JSON":
		ydbType = common.MakePrimitiveType(Ydb.Type_JSON)
	case typeName == "DATE":
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_DATETIME, rules.GetDateTimeFormat())
	case tm.isTimestamp.MatchString(typeName),
		tm.isTimestampWTZ.MatchString(typeName),
		tm.isTimestampWLTZ.MatchString(typeName):
		ydbType, err = common.MakeYdbDateTimeType(Ydb.Type_TIMESTAMP, rules.GetDateTimeFormat())
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

//nolint:gocyclo
func transformerFromSQLTypes(types []string, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	_ = ydbTypes
	acceptors := make([]any, 0, len(types))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(types))

	// there is a mismatch between the table metadata query and the go ora driver.
	// for some reason driver renames some types to its own names.
	// "LONG RAW" -> "LongRaw"
	// "BINARY_FLOAT" -> "IBFloat"
	// "BINARY_DOUBLE" -> "IBDouble"
	// "CLOB", "NCLOB" -> "LongVarChar"
	// "TIMESTAMP(*)" -> "TimeStampDTY"
	// "TIMESTAMP(*) WITH TIME ZONE" -> "TimeStampTZ_DTY"
	// "TIMESTAMP(*) WITH LOCAL TIME ZONE" -> "TimeStampLTZ_DTY"
	// "JSON" -> "OCIBlobLocator" (driver returns []byte)

	// Oracle data types:
	// 	https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6
	for i, typeName := range types {
		switch typeName {
		case "NUMBER":
			acceptors = append(acceptors, new(*int64))
			appenders = append(appenders, utils.MakeAppenderNullable[int64, int64, *array.Int64Builder](cc.Int64()))
		case "NCHAR", "CHAR", "LongVarChar", "LONG", "ROWID", "UROWID":
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		case "RAW", "LongRaw":
			acceptors = append(acceptors, new(*[]byte))
			appenders = append(appenders, utils.MakeAppenderNullable[[]byte, []byte, *array.BinaryBuilder](cc.Bytes()))
		case "OCIBlobLocator":
			ydbType := ydbTypes[i]

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			acceptors = append(acceptors, new(*[]byte))

			if ydbTypeID == Ydb.Type_JSON {
				appenders = append(appenders, utils.MakeAppenderNullable[[]byte, string, *array.StringBuilder](cc.BytesToString()))
			} else {
				appenders = append(appenders, utils.MakeAppenderNullable[[]byte, []byte, *array.BinaryBuilder](cc.Bytes()))
			}

		// YQ-3498: go-ora driver has a bug when reading BINARY_FLOAT -1.1, gives -1.2
		// case "IBFloat":
		// 	// driver giver float64 in driver.Value, also error while reading -1.1 (got -1.2)
		// 	acceptors = append(acceptors, new(*float32))
		// 	appenders = append(appenders, makeAppender[float32, float32, *array.Float32Builder](cc.Float32()))
		case "IBDouble":
			acceptors = append(acceptors, new(*float64))
			appenders = append(appenders, utils.MakeAppenderNullable[float64, float64, *array.Float64Builder](cc.Float64()))
		case "DATE":
			// Oracle Date value range is much more wide than YDB's Datetime value range
			ydbType := ydbTypes[i]

			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders,
					utils.MakeAppenderNullable[time.Time, string, *array.StringBuilder](cc.DatetimeToString()))
			case Ydb.Type_DATETIME:
				appenders = append(appenders, utils.MakeAppenderNullable[time.Time, uint32, *array.Uint32Builder](cc.Datetime()))
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
			}
		case "TimeStampDTY", "TimeStampTZ_DTY", "TimeStampLTZ_DTY": // TIMESTAMP
			// Oracle Timestamp value range is much more wide than YDB's Timestamp value range, and/or more precise
			ydbType := ydbTypes[i]

			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := common.YdbTypeToYdbPrimitiveTypeID(ydbType)
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders,
					utils.MakeAppenderNullable[time.Time, string, *array.StringBuilder](cc.TimestampToString(true)))
			case Ydb.Type_TIMESTAMP:
				appenders = append(appenders, utils.MakeAppenderNullable[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbType, typeName, common.ErrDataTypeNotSupported)
			}
		default:
			return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
		}
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
}

func NewTypeMapper() datasource.TypeMapper {
	return typeMapper{
		isTimestamp:     regexp.MustCompile(`TIMESTAMP\((.+)\)$`),
		isTimestampWTZ:  regexp.MustCompile(`TIMESTAMP\((.+)\) WITH TIME ZONE$`),
		isTimestampWLTZ: regexp.MustCompile(`TIMESTAMP\((.+)\) WITH LOCAL TIME ZONE$`),
	}
}
