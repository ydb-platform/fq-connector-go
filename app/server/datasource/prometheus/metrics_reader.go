package prometheus

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
)

type appenderFunc = func(acceptor any, builder array.Builder) error

type metricsReader struct {
	transformer paging.RowTransformer[any]

	arrowTypes *arrow.Schema
	ydbTypes   []*Ydb.Type
}

func makeMetricsReader(
	arrowTypes *arrow.Schema,
	ydbTypes []*Ydb.Type,
	cc conversion.Collection,
) (*metricsReader, error) {
	transformer, err := makeTransformer(ydbTypes, cc)
	if err != nil {
		return nil, fmt.Errorf("make transformer: %w", err)
	}

	return &metricsReader{
		transformer: transformer,
		arrowTypes:  arrowTypes,
		ydbTypes:    ydbTypes,
	}, nil
}

func makeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ydbTypes))
	appenders := make([]appenderFunc, 0, len(ydbTypes))

	var err error

	for _, ydbType := range ydbTypes {
		acceptors, appenders, err = addAcceptorAppender(ydbType, cc, acceptors, appenders)
		if err != nil {
			return nil, fmt.Errorf("add ydb type to acceptors and appenders: %w", err)
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
			return nil, nil, fmt.Errorf("add nullable: %w", err)
		}
	} else {
		acceptors, appenders, err = addAcceptorAppenderNonNullable(ydbType, cc, acceptors, appenders)
		if err != nil {
			return nil, nil, fmt.Errorf("add non nullable: %w", err)
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
		// Because only `string`, that represent prometheus labels, can be nullable
		case Ydb.Type_STRING:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, []byte, *array.BinaryBuilder](cc.StringToBytes()))
		default:
			return nil, nil, fmt.Errorf("unsupported typeid type: %s", t.TypeId.String())
		}
	default:
		return nil, nil, fmt.Errorf("unsupported type: %T", t)
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
		case Ydb.Type_DOUBLE:
			acceptors = append(acceptors, new(float64))
			appenders = append(appenders, utils.MakeAppender[float64, float64, *array.Float64Builder](cc.Float64()))
		case Ydb.Type_STRING:
			acceptors = append(acceptors, new(string))
			appenders = append(appenders, utils.MakeAppender[string, []byte, *array.BinaryBuilder](cc.StringToBytes()))
		case Ydb.Type_TIMESTAMP:
			acceptors = append(acceptors, new(time.Time))
			appenders = append(appenders, utils.MakeAppender[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
		default:
			return nil, nil, fmt.Errorf("unsupported typeid type: %s", t.TypeId.String())
		}
	default:
		return nil, nil, fmt.Errorf("unsupported type: %T", t)
	}

	return acceptors, appenders, nil
}

func convert[INTO any](acceptor **INTO, value any) {
	if v, ok := value.(INTO); ok {
		*acceptor = ptr.T[INTO](v)
		**acceptor = v
	} else {
		*acceptor = nil
	}
}

func (r *metricsReader) accept(l labels.Labels, timestamp int64, val float64) error {
	acceptors := r.transformer.GetAcceptors()

	for i, f := range r.arrowTypes.Fields() {
		switch a := acceptors[i].(type) {
		// Cause only timestamp column can be with time.Time type
		case *time.Time:
			*a = time.UnixMilli(timestamp)
		case **time.Time:
			convert(a, time.UnixMilli(timestamp))
		// Cause only value column can be with float64 type
		case *float64:
			*a = val
		case **float64:
			convert(a, val)
		// Cause only label columns can be with string type
		case *string:
			labelValue := l.Get(f.Name)
			if labelValue == "" {
				a = nil
				continue
			}

			*a = labelValue
		case **string:
			labelValue := l.Get(f.Name)
			if labelValue == "" {
				*a = nil
				continue
			}

			convert(a, labelValue)
		default:
			return fmt.Errorf("%w: %T", common.ErrDataTypeNotSupported, acceptors[i])
		}
	}

	return nil
}
