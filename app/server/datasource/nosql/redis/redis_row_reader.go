package redis

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
)

// redisRowReader преобразует сырые данные Redis в набор acceptors для RowTransformer.
type redisRowReader struct {
	transformer paging.RowTransformer[any]
	arrowSchema *arrow.Schema
	ydbTypes    []*Ydb.Type
	cc          conversion.Collection
}

// Создает новый redisRowReader, используя уже существующую функцию makeTransformer (аналогичную MongoDB).
func makeRedisRowReader(arrowSchema *arrow.Schema, ydbTypes []*Ydb.Type, cc conversion.Collection) (*redisRowReader, error) {
	transformer, err := makeTransformer(ydbTypes, cc)
	if err != nil {
		return nil, err
	}

	return &redisRowReader{
		transformer: transformer,
		arrowSchema: arrowSchema,
		ydbTypes:    ydbTypes,
		cc:          cc,
	}, nil
}

// accept преобразует данные строки (rowData) в массив acceptors в том же порядке, что и в arrowSchema.
func (r *redisRowReader) accept(logger *zap.Logger, rowData map[string]any) error {
	numCols := len(r.arrowSchema.Fields())
	acceptors := make([]any, numCols)

	for i, field := range r.arrowSchema.Fields() {
		switch field.Name {
		case KeyColumnName, StringColumnName:
			if v, ok := rowData[field.Name]; ok {
				acceptors[i] = v
			} else {
				acceptors[i] = nil
			}
		case HashColumnName:
			// Для колонки hashValues ожидаем, что rowData содержит map[string]string.
			if v, ok := rowData[HashColumnName].(map[string]string); ok {
				// Из ydbTypes[i] извлекаем информацию о структуре.
				st, ok := r.ydbTypes[i].Type.(*Ydb.Type_StructType)
				if !ok {
					return fmt.Errorf("expected struct type for column 'hashValues'")
				}

				structVal := make(map[string]*string)

				for _, member := range st.StructType.Members {
					if s, exists := v[member.Name]; exists {
						structVal[member.Name] = &s
					} else {
						structVal[member.Name] = nil
					}
				}

				acceptors[i] = structVal
			} else {
				acceptors[i] = nil
			}
		default:
			if v, ok := rowData[field.Name]; ok {
				acceptors[i] = v
			} else {
				acceptors[i] = nil
			}
		}
	}

	r.transformer.SetAcceptors(acceptors)

	return nil
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

func addAcceptorAppender(ydbType *Ydb.Type, cc conversion.Collection, acceptors []any, appenders []appenderFunc) (
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

func addAcceptorAppenderNullable(ydbType *Ydb.Type, cc conversion.Collection, acceptors []any, appenders []appenderFunc) (
	[]any,
	[]appenderFunc,
	error,
) {
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
		case Ydb.Type_DOUBLE:
			acceptors = append(acceptors, new(*float64))
			appenders = append(appenders, utils.MakeAppenderNullable[float64, float64, *array.Float64Builder](cc.Float64()))
		case Ydb.Type_STRING:
			acceptors = append(acceptors, new(*primitive.Binary))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				value := acceptor.(**primitive.Binary)
				if *value == nil {
					builder.AppendNull()
					return nil
				}

				return utils.AppendValueToArrowBuilder[[]byte, []byte, *array.BinaryBuilder](&(*value).Data, builder, cc.Bytes())
			})
		case Ydb.Type_UTF8, Ydb.Type_JSON:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		}

	case *Ydb.Type_TaggedType:
		if t.TaggedType.Tag == "ObjectId" {
			acceptors = append(acceptors, new(*primitive.ObjectID))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				value := acceptor.(**primitive.ObjectID)
				if *value == nil {
					builder.AppendNull()
					return nil
				}

				bytes, err := (*value).MarshalText()
				if err != nil {
					return err
				}

				return utils.AppendValueToArrowBuilder[[]byte, []byte, *array.BinaryBuilder](&bytes, builder, cc.Bytes())
			})
		} else {
			return nil, nil, fmt.Errorf("unknown Tagged tag: %s", t.TaggedType.Tag)
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
		case Ydb.Type_DOUBLE:
			acceptors = append(acceptors, new(float64))
			appenders = append(appenders, utils.MakeAppender[float64, float64, *array.Float64Builder](cc.Float64()))
		case Ydb.Type_STRING:
			acceptors = append(acceptors, new(primitive.Binary))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				value := acceptor.(*primitive.Binary)
				return utils.AppendValueToArrowBuilder[[]byte, []byte, *array.BinaryBuilder](&value.Data, builder, cc.Bytes())
			})
		case Ydb.Type_UTF8, Ydb.Type_JSON:
			acceptors = append(acceptors, new(string))
			appenders = append(appenders, utils.MakeAppender[string, string, *array.StringBuilder](cc.String()))
		}

	case *Ydb.Type_TaggedType:
		if t.TaggedType.Tag == "ObjectId" {
			acceptors = append(acceptors, new(primitive.ObjectID))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				value := acceptor.(*primitive.ObjectID)

				bytes, err := value.MarshalText()
				if err != nil {
					return err
				}

				return utils.AppendValueToArrowBuilder[[]byte, []byte, *array.BinaryBuilder](&bytes, builder, cc.Bytes())
			})
		} else {
			return nil, nil, fmt.Errorf("unknown Tagged tag: %s", t.TaggedType.Tag)
		}

	default:
		return nil, nil, fmt.Errorf("unsupported: %v", ydbType.String())
	}

	return acceptors, appenders, nil
}
