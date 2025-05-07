package opensearch

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
)

type documentReader struct {
	transformer paging.RowTransformer[any]

	arrowTypes *arrow.Schema
	ydbTypes   []*Ydb.Type
}

func convertToString(logger *zap.Logger, value any) (string, error) {
	switch cast := value.(type) {
	case int32:
		return strconv.Itoa(int(cast)), nil
	case int64:
		return strconv.FormatInt(cast, 10), nil
	case float32:
		return strconv.FormatFloat(float64(cast), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(cast, 'f', -1, 64), nil
	case string:
		return cast, nil
	case bool:
		return strconv.FormatBool(cast), nil
	case time.Time:
		return cast.Format(time.RFC3339), nil
	case []byte:
		return base64.StdEncoding.EncodeToString(cast), nil
	default:
		logger.Warn("unknown type", zap.Any("value", value))
	}

	return "", fmt.Errorf("unuspported type %T: %w", value, common.ErrDataTypeNotSupported)
}

//nolint:funlen,gocyclo
func (r *documentReader) accept(
	logger *zap.Logger,
	hit opensearchapi.SearchHit,
) error {
	var doc map[string]any
	// To unmarshal JSON into an interface value, Unmarshal stores one of these in the interface value:
	// bool, for JSON booleans
	// float64, for JSON numbers
	// string, for JSON strings
	// []any, for JSON arrays
	// map[string]any, for JSON objects
	// nil for JSON null

	if err := json.Unmarshal(hit.Source, &doc); err != nil {
		return fmt.Errorf("unmarshal _source: %w", err)
	}

	doc["_id"] = hit.ID

	acceptors := r.transformer.GetAcceptors()

	for i, f := range r.arrowTypes.Fields() {
		switch a := acceptors[i].(type) {
		case **uint8:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convertPtr[uint8](a, value); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case **bool:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convertPtr[bool](a, value); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case **int32:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convertPtr[int32](a, value.(float64)); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case **int64:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convertPtr[int64](a, value.(float64)); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case **float32:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convertPtr[float32](a, value.(float64)); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case **float64:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convertPtr[float64](a, value.(float64)); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case *string:
			if f.Name == "_id" {
				*a = hit.ID
			} else {
				return fmt.Errorf("unsupported type %T: for field %T, %w", acceptors[i], f.Name, common.ErrDataTypeNotSupported)
			}
		case **string:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			str, err := convertToString(logger, value)

			if err != nil {
				if !errors.Is(err, common.ErrDataTypeNotSupported) {
					return fmt.Errorf("json to string: %w", err)
				}
			}

			*a = ptr.T(str)
		case **time.Time:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			t, err := parseTime(value)
			if err != nil {
				return fmt.Errorf("parse time for field %s: %w", f.Name, err)
			}

			*a = ptr.T(t)
		case **map[string]any:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			convertedMap, err := convertToMapStringAny(value, f.Name)
			if err != nil {
				return fmt.Errorf("failed to convert map for field '%s': %w", f.Name, err)
			}

			*a = convertedMap
		default:
			return fmt.Errorf("unsupported type %T: %w for field %T", acceptors[i], common.ErrDataTypeNotSupported, f.Name)
		}
	}

	return nil
}

func convertToMapStringAny(value any, fieldName string) (*map[string]any, error) {
	inputMap, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any for field %s, got %T", fieldName, value)
	}

	resultMap := make(map[string]any, len(inputMap))

	for key, val := range inputMap {
		resultMap[key] = val
	}

	return &resultMap, nil
}

var (
	timeFormats = []string{
		time.DateOnly + "T" + time.TimeOnly, // "2006-01-02T15:04:05"
		time.DateOnly + " " + time.TimeOnly, // "2006-01-02 15:04:05"
	}
)

func parseTime(value any) (time.Time, error) {
	if value == nil {
		return time.Time{}, fmt.Errorf("time value is nil")
	}

	switch v := value.(type) {
	case string:
		// First try ZonedDateTime (with time zone)
		zonedTime, zerr := time.Parse(time.RFC3339Nano, v)
		if zerr == nil {
			return zonedTime, nil
		}

		// If it doesn't work, try LocalDateTime (without zone, interpret as UTC)
		localTime, lerr := time.Parse(time.DateTime, v)
		if lerr == nil {
			return localTime.UTC(), nil
		}

		for _, format := range timeFormats {
			t, err := time.Parse(format, v)
			if err == nil {
				return t.UTC(), nil
			}
		}

		return time.Time{}, fmt.Errorf("parse time string: %v", v)
	case float64:
		// Assume that these are milliseconds from the epoch
		sec := int64(v / 1000)
		nsec := int64(math.Round((v - math.Trunc(v/1000)*1000) * 1e6))

		return time.Unix(sec, nsec*1e3).UTC(), nil
	case int64:
		// Milliseconds from the epoch
		return time.Unix(0, v*int64(time.Millisecond)).UTC(), nil
	case time.Time:
		return v.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported time type: %T", value)
	}
}

func convertPtr[INTO any](acceptor **INTO, value any) error {
	if v, ok := value.(INTO); ok {
		*acceptor = ptr.T(v)
		return nil
	}

	var tmp INTO
	if err := convert(&tmp, value); err != nil {
		return err
	}

	*acceptor = ptr.T(tmp)

	return nil
}

// convert is a generic function that attempts to convert a value into the target type
// pointed to by acceptor. It handles special cases like float64 conversion for OpenSearch.
func convert[INTO any](acceptor *INTO, value any) error {
	// First try direct type assertion - if value is already of the desired type
	if v, ok := value.(INTO); ok {
		*acceptor = v

		return nil
	}

	// https://pkg.go.dev/encoding/json?spm=a2ty_o01.29997173.0.0.18cfc921mwu0YG#Unmarshal
	// To unmarshal JSON into an interface value, Unmarshal stores one of these in the interface value
	// float64, for JSON numbers
	if floatVal, ok := value.(float64); ok {
		switch pt := any(acceptor).(type) {
		case *uint8:
			*pt = uint8(floatVal)
		case *int32:
			*pt = int32(floatVal)
		case *int64:
			*pt = int64(floatVal)
		case *uint64:
			*pt = uint64(floatVal)
		case *float32:
			*pt = float32(floatVal)
		case *float64:
			*pt = floatVal
		default:
			return fmt.Errorf("unsupported conversion from float64 to %T", acceptor)
		}

		return nil
	}

	return fmt.Errorf("unsupported type %T: %w", value, common.ErrDataTypeNotSupported)
}

func makeDocumentReader(
	transformer paging.RowTransformer[any],
	arrowTypes *arrow.Schema,
	ydbTypes []*Ydb.Type,
) *documentReader {
	return &documentReader{
		transformer: transformer,
		arrowTypes:  arrowTypes,
		ydbTypes:    ydbTypes,
	}
}

type appenderFunc = func(acceptor any, builder array.Builder) error

func makeTransformer(
	ydbTypes []*Ydb.Type,
	cc conversion.Collection,
) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ydbTypes))
	appenders := make([]appenderFunc, 0, len(ydbTypes))

	var err error

	for _, ydbType := range ydbTypes {
		acceptors, appenders, err = addAcceptorAppender(ydbType, cc, acceptors, appenders)
		if err != nil {
			return nil, fmt.Errorf("add acceptor appender: %w", err)
		}
	}

	return paging.NewRowTransformer(acceptors, appenders, nil), nil
}

func addAcceptorAppender(
	ydbType *Ydb.Type,
	cc conversion.Collection,
	acceptors []any,
	appenders []appenderFunc,
) (
	[]any,
	[]appenderFunc,
	error,
) {
	var err error

	if optType := ydbType.GetOptionalType(); optType != nil {
		acceptors, appenders, err = addAcceptorAppenderNullable(optType.Item, cc, acceptors, appenders)
		if err != nil {
			return nil, nil, fmt.Errorf("add acceptor appender nullable: %w", err)
		}
	} else {
		acceptors, appenders, err = addAcceptorAppenderNonNullable(ydbType, cc, acceptors, appenders)
		if err != nil {
			return nil, nil, fmt.Errorf("add acceptor appender non nullable: %w", err)
		}
	}

	return acceptors, appenders, nil
}

func addAcceptorAppenderNullable(
	ydbType *Ydb.Type,
	cc conversion.Collection,
	acceptors []any,
	appenders []appenderFunc,
) ([]any, []appenderFunc, error) {
	switch t := ydbType.Type.(type) {
	case *Ydb.Type_TypeId:
		switch t.TypeId {
		case Ydb.Type_BOOL:
			acceptors = append(acceptors, new(*bool))
			appenders = append(appenders, utils.MakeAppenderNullable[bool, uint8, *array.Uint8Builder](cc.Bool()))
		case Ydb.Type_INT32:
			acceptors = append(acceptors, new(*int32))
			appenders = append(appenders, utils.MakeAppenderNullable[int32, int32, *array.Int32Builder](cc.Int32()))
		case Ydb.Type_INT64:
			acceptors = append(acceptors, new(*int64))
			appenders = append(appenders, utils.MakeAppenderNullable[int64, int64, *array.Int64Builder](cc.Int64()))
		case Ydb.Type_FLOAT:
			acceptors = append(acceptors, new(*float32))
			appenders = append(appenders, utils.MakeAppenderNullable[float32, float32, *array.Float32Builder](cc.Float32()))
		case Ydb.Type_DOUBLE:
			acceptors = append(acceptors, new(*float64))
			appenders = append(appenders, utils.MakeAppenderNullable[float64, float64, *array.Float64Builder](cc.Float64()))
		case Ydb.Type_UTF8:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		case Ydb.Type_STRING:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, []byte, *array.BinaryBuilder](cc.StringToBytes()))
		case Ydb.Type_TIMESTAMP:
			acceptors = append(acceptors, new(*time.Time))
			appenders = append(appenders, utils.MakeAppenderNullable[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
		}
	case *Ydb.Type_StructType:
		acceptors = append(acceptors, new(*map[string]any))
		appenders = append(appenders, createStructAppender(t.StructType))
	default:
		return nil, nil, fmt.Errorf("unsupported: %v", ydbType.String())
	}

	return acceptors, appenders, nil
}

//nolint:gocyclo
func createStructAppender(structType *Ydb.StructType) func(any, array.Builder) error {
	fieldNames := make([]string, len(structType.Members))
	for i, member := range structType.Members {
		fieldNames[i] = member.Name
	}

	return func(acceptor any, builder array.Builder) error {
		pt, ok := acceptor.(**map[string]any)
		if !ok {
			return fmt.Errorf("invalid acceptor type: expected **map[string]any, got %T", acceptor)
		}

		structBuilder, ok := builder.(*array.StructBuilder)
		if !ok {
			return fmt.Errorf("invalid builder type: expected *array.StructBuilder, got %T", builder)
		}

		if *pt == nil {
			structBuilder.AppendNull()
			return nil
		}

		structBuilder.Append(true) // Начинаем новую структуру

		data := *pt

		// Для каждого поля в структуре добавляем значение
		for fieldIdx := 0; fieldIdx < structBuilder.NumField(); fieldIdx++ {
			fieldName := builder.Type().(*arrow.StructType).Field(fieldIdx).Name
			fieldBuilder := structBuilder.FieldBuilder(fieldIdx)

			fieldValue := (*data)[fieldName]
			if fieldValue == nil {
				fieldBuilder.AppendNull()
				continue
			}

			switch fb := fieldBuilder.(type) {
			case *array.Uint8Builder:
				val, ok := fieldValue.(bool)
				if !ok {
					return fmt.Errorf("field %s: %w", fieldName, common.ErrDataTypeNotSupported)
				}

				fb.Append(uint8Bool(val))
			case *array.Int32Builder:
				val, err := anyToInt32(fieldValue)
				if err != nil {
					return fmt.Errorf("field %s: %w", fieldName, err)
				}

				fb.Append(val)
			case *array.Int64Builder:
				val, err := anyToInt64(fieldValue)
				if err != nil {
					return fmt.Errorf("field %s: %w", fieldName, err)
				}

				fb.Append(val)
			case *array.Uint64Builder:
				val, err := parseTime(fieldValue)
				if err != nil {
					return fmt.Errorf("field %s: %w", fieldName, err)
				}

				in, err := common.TimeToYDBTimestamp(&val)
				if err != nil {
					return fmt.Errorf("to timestamp %s: %w", fieldName, err)
				}

				fb.Append(in)
			case *array.Float32Builder:
				val, err := anyToFloat32(fieldValue)
				if err != nil {
					return fmt.Errorf("field %s: %w", fieldName, err)
				}

				fb.Append(val)
			case *array.Float64Builder:
				val, err := anyToFloat64(fieldValue)
				if err != nil {
					return fmt.Errorf("field %s: %w", fieldName, err)
				}

				fb.Append(val)
			case *array.StringBuilder:
				strval, ok := fieldValue.(string)
				if !ok {
					return fmt.Errorf("field %s: expected string but got %T", fieldName, fieldValue)
				}

				fb.Append(strval)
			case *array.BinaryBuilder:
				strval, ok := fieldValue.(string)
				if !ok {
					return fmt.Errorf("field %s: expected binary but got %T", fieldName, fieldValue)
				}

				fb.Append([]byte(strval))
			default:
				return fmt.Errorf("unsupported builder type %T for field %s", fb, fieldName)
			}
		}

		return nil
	}
}

func anyToInt32(v any) (int32, error) {
	switch val := v.(type) {
	case float64:
		return int32(val), nil
	case int32:
		return val, nil
	default:
		var res int32
		if err := convert(&res, v); err != nil {
			return 0, fmt.Errorf("cannot convert %T to int32: %w", v, err)
		}

		return res, nil
	}
}

func anyToInt64(v any) (int64, error) {
	switch val := v.(type) {
	case float64:
		return int64(val), nil
	case int64:
		return val, nil
	default:
		var res int64
		if err := convert(&res, v); err != nil {
			return 0, fmt.Errorf("cannot convert %T to int64: %w", v, err)
		}

		return res, nil
	}
}

func anyToFloat32(v any) (float32, error) {
	switch val := v.(type) {
	case float64:
		return float32(val), nil
	case float32:
		return val, nil
	default:
		var res float32
		if err := convert(&res, v); err != nil {
			return 0, fmt.Errorf("cannot convert %T to float32: %w", v, err)
		}

		return res, nil
	}
}

func anyToFloat64(v any) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	default:
		var res float64
		if err := convert(&res, v); err != nil {
			return 0, fmt.Errorf("cannot convert %T to float64: %w", v, err)
		}

		return res, nil
	}
}

func uint8Bool(b bool) uint8 {
	if b {
		return 1
	}

	return 0
}

func addAcceptorAppenderNonNullable(
	ydbType *Ydb.Type,
	cc conversion.Collection,
	acceptors []any,
	appenders []appenderFunc,
) (
	[]any,
	[]appenderFunc,
	error,
) {
	switch t := ydbType.Type.(type) {
	case *Ydb.Type_TypeId:
		if t.TypeId == Ydb.Type_UTF8 {
			acceptors = append(acceptors, new(string))
			appenders = append(appenders, utils.MakeAppender[string, string, *array.StringBuilder](cc.String()))
		}
	default:
		return nil, nil, fmt.Errorf("unsupported: %v", ydbType.String())
	}

	return acceptors, appenders, nil
}
