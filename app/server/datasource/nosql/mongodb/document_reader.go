package mongodb

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.ytsaurus.tech/yt/go/yson"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
)

type unexpectedTypeDisplayMode = api_common.TMongoDbDataSourceOptions_EUnexpectedTypeDisplayMode

type documentReader struct {
	transformer paging.RowTransformer[any]

	readingMode           readingMode
	unexpectedDisplayMode unexpectedTypeDisplayMode

	arrowTypes *arrow.Schema
	ydbTypes   []*Ydb.Type
}

func isSerializedDocumentReadingMode(readingMode readingMode) bool {
	switch readingMode {
	case api_common.TMongoDbDataSourceOptions_JSON,
		api_common.TMongoDbDataSourceOptions_YSON:
		return true
	default:
		return false
	}
}

func bsonToString(value any) (string, error) {
	switch cast := value.(type) {
	case int32:
		return strconv.Itoa(int(cast)), nil
	case int64:
		return strconv.FormatInt(cast, 10), nil
	case float64:
		return strconv.FormatFloat(cast, 'f', -1, 64), nil
	case string:
		return cast, nil
	case bool:
		return strconv.FormatBool(cast), nil
	case primitive.ObjectID:
		text, err := cast.MarshalText()
		if err != nil {
			return "", err
		}

		return string(text), nil
	case primitive.DateTime:
		return cast.Time().String(), common.ErrDataTypeNotSupported
	case primitive.Decimal128:
		return cast.String(), common.ErrDataTypeNotSupported
	case primitive.Binary:
		return base64.StdEncoding.EncodeToString(cast.Data), nil
	case primitive.A:
		return bsonAToString(cast)
	case primitive.M:
		return bsonMToString(cast)
	case primitive.D:
		return bsonDToString(cast)
	default:
	}

	return "", common.ErrDataTypeNotSupported
}

func bsonAToString(arr bson.A) (string, error) {
	var sb strings.Builder

	sb.WriteString("[")

	for i, inner := range arr {
		innerStr, err := bsonToString(inner)
		if err != nil {
			return "", err
		}

		sb.WriteString(innerStr)

		if i+1 < len(arr) {
			sb.WriteString(", ")
		}
	}

	sb.WriteString("]")

	return sb.String(), common.ErrDataTypeNotSupported
}

func bsonMToString(m bson.M) (string, error) {
	var sb strings.Builder

	sb.WriteString("{")

	i := 0

	for key, value := range m {
		valueStr, err := bsonToString(value)
		if err != nil {
			return "", err
		}

		i++

		sb.WriteString(fmt.Sprintf("%s: %s", key, valueStr))

		if i+1 < len(m) {
			sb.WriteString(", ")
		}
	}

	sb.WriteString("}")

	return sb.String(), common.ErrDataTypeNotSupported
}

func bsonDToString(doc bson.D) (string, error) {
	var sb strings.Builder

	sb.WriteString("{")

	for i, elem := range doc {
		valueStr, err := bsonToString(elem.Value)
		if err != nil {
			return "", err
		}

		i++

		sb.WriteString(fmt.Sprintf("%s: %s", elem.Value, valueStr))

		if i+1 < len(doc) {
			sb.WriteString(", ")
		}
	}

	sb.WriteString("}")

	return sb.String(), common.ErrDataTypeNotSupported
}

func convert[INTO any](acceptor **INTO, value any) {
	if v, ok := value.(INTO); ok {
		*acceptor = ptr.T(v)
	} else {
		*acceptor = nil
	}
}

func (r *documentReader) accept(doc bson.M) error {
	acceptors := r.transformer.GetAcceptors()

	if isSerializedDocumentReadingMode(r.readingMode) {
		if len(r.arrowTypes.Fields()) != 2 {
			return fmt.Errorf("unexpected number of accepters for a serialized document reading mode")
		}

		for i, f := range r.arrowTypes.Fields() {
			if f.Name == idColumn {
				if err := r.acceptSingleField(acceptors[i], doc, f.Name); err != nil {
					return err
				}
			} else {
				if err := r.acceptSerializedDocument(acceptors[i], doc); err != nil {
					return err
				}
			}
		}

		return nil
	}

	for i, f := range r.arrowTypes.Fields() {
		if err := r.acceptSingleField(acceptors[i], doc, f.Name); err != nil {
			return err
		}
	}

	return nil
}

func (r *documentReader) acceptSerializedDocument(acceptor any, doc bson.M) error {
	switch r.readingMode {
	case api_common.TMongoDbDataSourceOptions_JSON:
		b, err := json.Marshal(doc)
		if err != nil {
			return err
		}

		a, ok := acceptor.(*string)
		if !ok {
			return fmt.Errorf("unexpected acceptor type for JSON serialized document: %T", acceptor)
		}

		*a = string(b)

	case api_common.TMongoDbDataSourceOptions_YSON:
		b, err := yson.Marshal(doc)
		if err != nil {
			return err
		}

		a, ok := acceptor.(*[]byte)
		if !ok {
			return fmt.Errorf("unexpected acceptor type for YSON serialized document: %T", acceptor)
		}

		*a = b

	default:
		return fmt.Errorf("unexpected reading mode for serialized document accepter: %v", r.readingMode)
	}

	return nil
}

//nolint:funlen,gocyclo
func (r *documentReader) acceptSingleField(acceptor any, doc bson.M, fieldName string) error {
	switch a := acceptor.(type) {
	case *bool:
		*a = doc[fieldName].(bool)
	case **bool:
		convert(a, doc[fieldName])
	case *int32:
		*a = doc[fieldName].(int32)
	case **int32:
		convert(a, doc[fieldName])
	case *int64:
		*a = doc[fieldName].(int64)
	case **int64:
		convert(a, doc[fieldName])
	case *float64:
		*a = doc[fieldName].(float64)
	case **float64:
		convert(a, doc[fieldName])
	case *string:
		value, ok := doc[fieldName]
		if !ok {
			return nil
		}

		str, err := bsonToString(value)
		if err != nil {
			if !errors.Is(err, common.ErrDataTypeNotSupported) {
				return err
			}

			if r.unexpectedDisplayMode == api_common.TMongoDbDataSourceOptions_UNEXPECTED_AS_NULL {
				return nil
			}
		}

		*a = str

	case **string:
		value, ok := doc[fieldName]
		if !ok {
			*a = nil
			return nil
		}

		str, err := bsonToString(value)
		if err != nil {
			if !errors.Is(err, common.ErrDataTypeNotSupported) {
				return err
			}

			if r.unexpectedDisplayMode == api_common.TMongoDbDataSourceOptions_UNEXPECTED_AS_NULL {
				*a = nil
				return nil
			}
		}

		*a = ptr.T(str)

	case *primitive.Binary:
		*a = doc[fieldName].(primitive.Binary)
	case **primitive.Binary:
		convert(a, doc[fieldName])
	case *primitive.ObjectID:
		*a = doc[fieldName].(primitive.ObjectID)
	case **primitive.ObjectID:
		convert(a, doc[fieldName])
	case *any:
		// We use any to handle both ObjectID and Binary BSON types when converting them to YQL String.
		value, ok := doc[fieldName]
		if !ok {
			return nil
		}

		switch value.(type) {
		case primitive.Binary:
			*a = value
		case primitive.ObjectID:
			*a = value
		default:
			return fmt.Errorf("unuspported type %T: %w", value, common.ErrDataTypeNotSupported)
		}

	case **any:
		// We use any to handle both ObjectID and Binary BSON types when converting them to YQL String.
		value, ok := doc[fieldName]
		if !ok {
			*a = nil
			return nil
		}

		switch value.(type) {
		case primitive.Binary:
			*a = ptr.T(value)
		case primitive.ObjectID:
			*a = ptr.T(value)
		default:
			return fmt.Errorf("unuspported type %T: %w", value, common.ErrDataTypeNotSupported)
		}
	}

	return nil
}

func makeDocumentReader(
	readingMode readingMode,
	unexpectedDisplayMode unexpectedTypeDisplayMode,
	arrowTypes *arrow.Schema,
	ydbTypes []*Ydb.Type,
	cc conversion.Collection,
) (*documentReader, error) {
	transformer, err := makeTransformer(ydbTypes, cc)
	if err != nil {
		return nil, err
	}

	return &documentReader{
		transformer:           transformer,
		readingMode:           readingMode,
		unexpectedDisplayMode: unexpectedDisplayMode,
		arrowTypes:            arrowTypes,
		ydbTypes:              ydbTypes,
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
		case Ydb.Type_UTF8, Ydb.Type_JSON:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		case Ydb.Type_YSON:
			acceptors = append(acceptors, new(*[]byte))
			appenders = append(appenders, utils.MakeAppenderNullable[[]byte, []byte, *array.BinaryBuilder](cc.Bytes()))
		case Ydb.Type_STRING:
			// When reading data from MongoDB, we sometimes encounter two different BSON types
			// (ObjectId and Binary) that both need to be converted to the same YQL String type.
			// Since we don't know in advance which type we'll get,
			// we use Go's any (empty interface) to handle both possibilities.
			// This is the simplest approach to deal with this type ambiguity during the conversion process.
			acceptors = append(acceptors, new(*any))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				acceptorPtr := acceptor.(**any)
				if *acceptorPtr == nil {
					builder.AppendNull()
					return nil
				}

				return yqlStringAppender(**acceptorPtr, builder, cc.Bytes())
			})
		}

	case *Ydb.Type_TaggedType:
		if t.TaggedType.Tag == objectIdTag {
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
		case Ydb.Type_UTF8, Ydb.Type_JSON:
			acceptors = append(acceptors, new(string))
			appenders = append(appenders, utils.MakeAppender[string, string, *array.StringBuilder](cc.String()))
		case Ydb.Type_YSON:
			acceptors = append(acceptors, new([]byte))
			appenders = append(appenders, utils.MakeAppender[[]byte, []byte, *array.BinaryBuilder](cc.Bytes()))
		case Ydb.Type_STRING:
			// When reading data from MongoDB, we sometimes encounter two different BSON types
			// (ObjectId and Binary) that both need to be converted to the same YQL String type.
			// Since we don't know in advance which type we'll get,
			// we use Go's any (empty interface) to handle both possibilities.
			// This is the simplest approach to deal with this type ambiguity during the conversion process.
			acceptors = append(acceptors, new(any))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				acceptorPtr := acceptor.(*any)
				if acceptorPtr == nil {
					builder.AppendNull()
					return nil
				}

				return yqlStringAppender(*acceptorPtr, builder, cc.Bytes())
			})
		}

	case *Ydb.Type_TaggedType:
		if t.TaggedType.Tag == objectIdTag {
			acceptors = append(acceptors, new(primitive.ObjectID))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				value := acceptor.(*primitive.ObjectID)

				bytes, err := value.MarshalText()
				if err != nil {
					return fmt.Errorf("marshal text from data in ObjectId: %w", err)
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

func yqlStringAppender(acceptor any, builder array.Builder, ccBytes conversion.ValuePtrConverter[[]byte, []byte]) error {
	switch value := (acceptor).(type) {
	case primitive.Binary:
		return utils.AppendValueToArrowBuilder[[]byte, []byte, *array.BinaryBuilder](&value.Data, builder, ccBytes)
	case primitive.ObjectID:
		bytes, err := value.MarshalText()
		if err != nil {
			return fmt.Errorf("marshal text from data in ObjectId: %w", err)
		}

		return utils.AppendValueToArrowBuilder[[]byte, []byte, *array.BinaryBuilder](&bytes, builder, ccBytes)
	default:
		return fmt.Errorf("unsupported type mapped to YQL STRING: %v", value)
	}
}
