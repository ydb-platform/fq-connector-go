package opensearch

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

func jsonToString(logger *zap.Logger, value any) (string, error) {
	logger.Debug("jsonToString", zap.Any("value", value))

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
		logger.Warn(fmt.Sprintf("unknown type: %T", value))
	}

	return "", common.ErrDataTypeNotSupported
}

//nolint:funlen,gocyclo
func (r *documentReader) accept(logger *zap.Logger, hit opensearchapi.SearchHit) error {
	var doc map[string]any
	if err := json.Unmarshal(hit.Source, &doc); err != nil {
		return fmt.Errorf("unmarshal _source: %w", err)
	}

	doc["_id"] = hit.ID

	acceptors := r.transformer.GetAcceptors()
	logger.Debug("accept document", zap.Any("acceptors", acceptors), zap.Any("fields", r.arrowTypes.Fields()))

	for i, f := range r.arrowTypes.Fields() {
		logger.Debug(fmt.Sprintf("accept field %d: %s", i, f))

		switch a := acceptors[i].(type) {
		case **bool:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convert[bool](a, value); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case **int32:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convert[int32](a, value.(float64)); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case **int64:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convert[int64](a, value.(float64)); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case **float32:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convert[float32](a, value.(float64)); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case **float64:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			if err := convert[float64](a, value.(float64)); err != nil {
				return fmt.Errorf("convert: %w", err)
			}
		case *string:
			if f.Name == "_id" {
				*a = hit.ID
			} else {
				return fmt.Errorf("unsupported type %T: %w for field %T", acceptors[i], common.ErrDataTypeNotSupported, f.Name)
			}
		case **string:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			str, err := jsonToString(logger, value)

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
		case **map[string]string:
			value, ok := doc[f.Name]
			if !ok {
				*a = nil
				continue
			}

			convertedMap, err := convertToMapStringString(logger, value, f.Name)
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

func convertToMapStringString(logger *zap.Logger, value any, fieldName string) (*map[string]string, error) {
	inputMap, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any for field %s, got %T", fieldName, value)
	}

	resultMap := make(map[string]string, len(inputMap))

	for key, val := range inputMap {
		strVal, err := jsonToString(logger, val)
		if err != nil {
			if errors.Is(err, common.ErrDataTypeNotSupported) {
				logger.Warn("unsupported type in map conversion",
					zap.String("field", fieldName),
					zap.String("key", key),
					zap.Any("value", val))

				continue
			}

			return nil, fmt.Errorf("convert map value for field %s.%s: %w", fieldName, key, err)
		}

		resultMap[key] = strVal
	}

	return &resultMap, nil
}

func parseTime(value any) (time.Time, error) {
	if value == nil {
		return time.Time{}, fmt.Errorf("time value is nil")
	}

	switch v := value.(type) {
	case string:
		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}

		return time.Time{}, fmt.Errorf("parse time string: %s", v)

	case float64:
		return time.Unix(0, int64(v)*int64(time.Millisecond)), nil

	case int64:
		return time.Unix(0, v), nil

	case time.Time:
		return v, nil

	default:
		return time.Time{}, fmt.Errorf("unsupported time type %T", value)
	}
}

func convert[INTO any](acceptor **INTO, value any) error {
	if v, ok := value.(INTO); ok {
		*acceptor = ptr.T(v)
		return nil
	}

	if floatVal, ok := value.(float64); ok {
		switch pt := any(acceptor).(type) {
		case **int32:
			*pt = ptr.T(int32(floatVal))
			return nil
		case **int64:
			*pt = ptr.T(int64(floatVal))
			return nil
		case **float32:
			*pt = ptr.T(float32(floatVal))
			return nil
		case **float64:
			*pt = ptr.T(floatVal)
			return nil
		}
	}

	return fmt.Errorf("unsupported type %T: %w", value, common.ErrDataTypeNotSupported)
}

func makeDocumentReader(
	transformer paging.RowTransformer[any],
	arrowTypes *arrow.Schema,
	ydbTypes []*Ydb.Type,
) (*documentReader, error) {
	return &documentReader{
		transformer: transformer,
		arrowTypes:  arrowTypes,
		ydbTypes:    ydbTypes,
	}, nil
}

type appenderFunc = func(acceptor any, builder array.Builder) error

func makeTransformer(
	logger *zap.Logger,
	ydbTypes []*Ydb.Type,
	cc conversion.Collection,
) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ydbTypes))
	appenders := make([]appenderFunc, 0, len(ydbTypes))

	var err error

	for _, ydbType := range ydbTypes {
		acceptors, appenders, err = addAcceptorAppender(logger, ydbType, cc, acceptors, appenders)
		if err != nil {
			return nil, fmt.Errorf("addAcceptorAppender: %w", err)
		}
	}

	logger.Debug("addAcceptorAppender", zap.Any("acceptors", acceptors))

	return paging.NewRowTransformer(acceptors, appenders, nil), nil
}

func addAcceptorAppender(
	logger *zap.Logger,
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
			return nil, nil, fmt.Errorf("addAcceptorAppenderNullable: %w", err)
		}

		logger.Debug(fmt.Sprintf("addAcceptorAppenderNullable type: %T", optType.Item.Type))
	} else {
		acceptors, appenders, err = addAcceptorAppenderNonNullable(ydbType, cc, acceptors, appenders)
		if err != nil {
			return nil, nil, fmt.Errorf("addAcceptorAppenderNonNullable: %w", err)
		}

		logger.Debug(fmt.Sprintf("addAcceptorAppenderNonNullable type: %T", ydbType.Type))
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
		case Ydb.Type_STRING:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, []byte, *array.BinaryBuilder](cc.StringToBytes()))
		case Ydb.Type_TIMESTAMP:
			acceptors = append(acceptors, new(*time.Time))
			appenders = append(appenders, utils.MakeAppenderNullable[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
		}
	case *Ydb.Type_StructType:
		acceptors = append(acceptors, new(*map[string]string))
		appenders = append(appenders, createStructAppender(t.StructType))
	default:
		return nil, nil, fmt.Errorf("unsupported: %v", ydbType.String())
	}

	return acceptors, appenders, nil
}

func createStructAppender(structType *Ydb.StructType) func(any, array.Builder) error {
	fieldNames := make([]string, len(structType.Members))
	for i, member := range structType.Members {
		fieldNames[i] = member.Name
	}

	return func(acceptor any, builder array.Builder) error {
		pt, ok := acceptor.(**map[string]string)
		if !ok {
			return fmt.Errorf("invalid acceptor type: expected **map[string]string, got %T", acceptor)
		}

		structBuilder, ok := builder.(*array.StructBuilder)
		if !ok {
			return fmt.Errorf("invalid builder type: expected *array.StructBuilder, got %T", builder)
		}

		if pt == nil || *pt == nil {
			structBuilder.AppendNull()
			return nil
		}

		structBuilder.Append(true)

		data := *pt

		for i, fieldName := range fieldNames {
			fieldBuilder, ok := structBuilder.FieldBuilder(i).(*array.BinaryBuilder)
			if !ok {
				return fmt.Errorf("unexpected builder type for field %s: %T", fieldName, structBuilder.FieldBuilder(i))
			}

			if value, exists := (*data)[fieldName]; exists {
				fieldBuilder.Append([]byte(value))
			} else {
				fieldBuilder.AppendNull()
			}
		}

		return nil
	}
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
		if t.TypeId == Ydb.Type_STRING {
			acceptors = append(acceptors, new(string))
			appenders = append(appenders, utils.MakeAppender[string, []byte, *array.BinaryBuilder](cc.StringToBytes()))
		}
	default:
		return nil, nil, fmt.Errorf("unsupported: %v", ydbType.String())
	}

	return acceptors, appenders, nil
}
