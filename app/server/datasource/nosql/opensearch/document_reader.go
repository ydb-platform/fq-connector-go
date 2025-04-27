package opensearch

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	case []any:
		return jsonArrayToString(logger, cast)
	case map[string]any:
		return jsonObjectToString(logger, cast)
	default:
		logger.Info(fmt.Sprintf("unknown type: %T", value))
	}

	return "", common.ErrDataTypeNotSupported
}

func jsonArrayToString(logger *zap.Logger, arr []any) (string, error) {
	var sb strings.Builder

	sb.WriteString("[")

	for i, inner := range arr {
		innerStr, err := jsonToString(logger, inner)
		if err != nil {
			return "", err
		}

		sb.WriteString(innerStr)

		if i+1 < len(arr) {
			sb.WriteString(", ")
		}
	}

	sb.WriteString("]")

	return sb.String(), nil
}

func jsonObjectToString(logger *zap.Logger, obj map[string]any) (string, error) {
	var sb strings.Builder

	sb.WriteString("{")

	i := 0

	for key, value := range obj {
		valueStr, err := jsonToString(logger, value)
		if err != nil {
			return "", err
		}

		if i > 0 {
			sb.WriteString(", ")
		}

		sb.WriteString(fmt.Sprintf(`"%s": %s`, key, valueStr))

		i++
	}

	sb.WriteString("}")

	return sb.String(), nil
}

//nolint:funlen,gocyclo
func (r *documentReader) accept(logger *zap.Logger, hit opensearchapi.SearchHit) error {
	var doc map[string]any
	if err := json.Unmarshal(hit.Source, &doc); err != nil {
		return fmt.Errorf("unmarshal _source: %w", err)
	}

	acceptors := r.transformer.GetAcceptors()

	for i, f := range r.arrowTypes.Fields() {
		switch a := acceptors[i].(type) {
		case *bool:
			*a = doc[f.Name].(bool)
		case **bool:
			convert(a, doc[f.Name])
		case *int32:
			if v, ok := doc[f.Name].(float64); ok {
				*a = int32(v)
			}
		case **int32:
			if v, ok := doc[f.Name].(float64); ok {
				val := int32(v)
				*a = &val
			} else {
				*a = nil
			}
		case *int64:
			if v, ok := doc[f.Name].(float64); ok {
				*a = int64(v)
			}
		case **int64:
			if v, ok := doc[f.Name].(float64); ok {
				val := int64(v)
				*a = &val
			} else {
				*a = nil
			}
		case *float32:
			*a = doc[f.Name].(float32)
		case **float32:
			convert(a, doc[f.Name])
		case *float64:
			*a = doc[f.Name].(float64)
		case **float64:
			convert(a, doc[f.Name])
		case *string:
			value, ok := doc[f.Name]
			if !ok {
				acceptors[i] = nil
				continue
			}

			str, err := jsonToString(logger, value)
			if err != nil {
				if !errors.Is(err, common.ErrDataTypeNotSupported) {
					return fmt.Errorf("jsonToString: %w", err)
				}
			}

			*a = str
		case **string:
			value, ok := doc[f.Name]
			if !ok {
				acceptors[i] = nil
				*a = nil

				continue
			}

			str, err := jsonToString(logger, value)
			if err != nil {
				if !errors.Is(err, common.ErrDataTypeNotSupported) {
					return fmt.Errorf("jsonToString: %w", err)
				}
			}

			*a = ptr.T(str)
		default:
			return fmt.Errorf("unsupported type %T: %w", acceptors[i], common.ErrDataTypeNotSupported)
		}
	}

	return nil
}

func convert[INTO any](acceptor **INTO, value any) {
	if v, ok := value.(INTO); ok {
		*acceptor = ptr.T(v)
	} else {
		*acceptor = nil
	}
}

func makeDocumentReader(
	arrowTypes *arrow.Schema,
	ydbTypes []*Ydb.Type,
	cc conversion.Collection,
) (*documentReader, error) {
	transformer, err := makeTransformer(ydbTypes, cc)
	if err != nil {
		return nil, err
	}

	return &documentReader{
		transformer: transformer,
		arrowTypes:  arrowTypes,
		ydbTypes:    ydbTypes,
	}, nil
}

type appenderFunc = func(acceptor any, builder array.Builder) error

func makeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ydbTypes))
	appenders := make([]appenderFunc, 0, len(ydbTypes))

	var err error

	for _, ydbType := range ydbTypes {
		acceptors, appenders, err = addAcceptorAppender(ydbType, cc, acceptors, appenders)
		if err != nil {
			return nil, fmt.Errorf("addAcceptorAppender: %w", err)
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
			return nil, nil, err
		}
	} else {
		acceptors, appenders, err = addAcceptorAppenderNonNullable(ydbType, cc, acceptors, appenders)
		if err != nil {
			return nil, nil, err
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
		case Ydb.Type_UTF8, Ydb.Type_JSON:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		case Ydb.Type_STRING:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				value := acceptor.(**string)
				if *value == nil {
					builder.AppendNull()
					return nil
				}

				return utils.AppendValueToArrowBuilder[string, string, *array.StringBuilder](
					*value,
					builder,
					cc.String(),
				)
			})
		case Ydb.Type_TIMESTAMP:
			acceptors = append(acceptors, new(*time.Time))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				value := acceptor.(**time.Time)
				if *value == nil {
					builder.AppendNull()
					return nil
				}

				return utils.AppendValueToArrowBuilder[int64, int64, *array.Int64Builder](
					(**value).UnixNano(),
					builder,
					cc.Int64(),
				)
			})
		}

	default:
		return nil, nil, fmt.Errorf("unsupported: %v", ydbType.String())
	}

	return acceptors, appenders, nil
}

func addAcceptorAppenderNonNullable(ydbType *Ydb.Type, cc conversion.Collection, acceptors []any, appenders []appenderFunc) (
	[]any,
	[]appenderFunc,
	error,
) {
	switch t := ydbType.Type.(type) {
	case *Ydb.Type_TypeId:
		switch t.TypeId {
		case Ydb.Type_BOOL:
			acceptors = append(acceptors, new(bool))
			appenders = append(appenders, utils.MakeAppender[bool, uint8, *array.Uint8Builder](cc.Bool()))
		case Ydb.Type_INT32:
			acceptors = append(acceptors, new(int32))
			appenders = append(appenders, utils.MakeAppender[int32, int32, *array.Int32Builder](cc.Int32()))
		case Ydb.Type_INT64:
			acceptors = append(acceptors, new(int64))
			appenders = append(appenders, utils.MakeAppender[int64, int64, *array.Int64Builder](cc.Int64()))
		case Ydb.Type_FLOAT:
			acceptors = append(acceptors, new(float32))
			appenders = append(appenders, utils.MakeAppender[float32, float32, *array.Float32Builder](cc.Float32()))
		case Ydb.Type_DOUBLE:
			acceptors = append(acceptors, new(float64))
			appenders = append(appenders, utils.MakeAppender[float64, float64, *array.Float64Builder](cc.Float64()))
		case Ydb.Type_UTF8, Ydb.Type_JSON, Ydb.Type_STRING:
			acceptors = append(acceptors, new(any))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				value := acceptor.(*any)

				return utils.AppendValueToArrowBuilder[[]byte, []byte, *array.BinaryBuilder](
					&value,
					builder,
					cc.Bytes(),
				)
			})
		case Ydb.Type_TIMESTAMP:
			acceptors = append(acceptors, new(time.Time))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				value := acceptor.(*time.Time)
				nanos := value.UnixNano()

				return utils.AppendValueToArrowBuilder[int64, int64, *array.Int64Builder](&nanos, builder, cc.Int64())
			})
		}

	default:
		return nil, nil, fmt.Errorf("unsupported: %v", ydbType.String())
	}

	return acceptors, appenders, nil
}
