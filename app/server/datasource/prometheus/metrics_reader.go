package prometheus

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"
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
		return nil, err
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
		acceptors, appenders, err = addAcceptorAppenderNonNullable(ydbType, cc, acceptors, appenders)
		if err != nil {
			return nil, fmt.Errorf("addAcceptorAppender: %w", err)
		}
	}

	return paging.NewRowTransformer(acceptors, appenders, nil), nil
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
			acceptors = append(acceptors, new(uint64))
			appenders = append(appenders, utils.MakeAppender[uint64, uint64, *array.Uint64Builder](cc.Uint64()))
		}
	default:
		return nil, nil, fmt.Errorf("unsupported: %v", ydbType.String())
	}

	return acceptors, appenders, nil
}

func convert[INTO any](acceptor **INTO, value any) {
	if v, ok := value.(INTO); ok {
		*acceptor = new(INTO)
		**acceptor = v
	} else {
		*acceptor = nil
	}
}

func (r *metricsReader) accept(logger *zap.Logger, l labels.Labels, timestamp int64, val float64) error {
	acceptors := r.transformer.GetAcceptors()

	for i, f := range r.arrowTypes.Fields() {
		switch a := acceptors[i].(type) {
		// Cause only timestamp column can be with uint64 type
		case *uint64:
			*a = uint64(timestamp)
		case **uint64:
			convert(a, uint64(timestamp))
		// Cause only value column can be with float64 type
		case *float64:
			*a = val
		case **float64:
			convert(a, val)
		// Cause only label columns can be with string type
		case *string:
			labelValue := l.Get(f.Name)
			if labelValue == "" {
				continue
			}

			*a = labelValue
		case **string:
			labelValue := l.Get(f.Name)
			if labelValue == "" {
				continue
			}

			*a = new(string)
			**a = labelValue
		default:
			logger.Warn(fmt.Sprintf("unsupported %T", acceptors[i]))

			return common.ErrDataTypeNotSupported
		}
	}

	r.transformer.SetAcceptors(acceptors)

	return nil
}
