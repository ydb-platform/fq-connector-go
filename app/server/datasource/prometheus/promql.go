package prometheus

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

const (
	prometheusNameLabel = "__name__"
)

var acceptableErrors = common.NewErrorMatcher(
	common.ErrUnsupportedExpression,
	common.ErrUnimplementedPredicateType,
	common.ErrUnimplementedTypedValue,
	common.ErrUnimplementedOperation,
)

type PromQLBuilder struct {
	logger        *zap.Logger
	labelMatchers []*labels.Matcher
	startTime     int64
	endTime       int64

	predicateErrors []error
}

func NewPromQLBuilder(logger *zap.Logger) PromQLBuilder {
	return PromQLBuilder{
		logger: logger,
		// Because we will at least add the `from` expression.
		labelMatchers: make([]*labels.Matcher, 0, 1),
		// By default, we collect all metrics before `startTime`
		endTime: toPromTime(time.Now().Add(time.Hour)),

		predicateErrors: make([]error, 0),
	}
}

func (p PromQLBuilder) From(from string) PromQLBuilder {
	p.labelMatchers = append(p.labelMatchers, &labels.Matcher{
		Type:  labels.MatchEqual,
		Name:  prometheusNameLabel,
		Value: from,
	})

	return p
}

func (p PromQLBuilder) WithStartTime(start time.Time) PromQLBuilder {
	p.startTime = toPromTime(start)
	return p
}

func (p PromQLBuilder) WithEndTime(end time.Time) PromQLBuilder {
	p.endTime = toPromTime(end)
	return p
}

func (p PromQLBuilder) WithYdbWhere(where *protos.TSelect_TWhere, filtering protos.TReadSplitsRequest_EFiltering) (PromQLBuilder, error) {
	// If Where clause is not provided, return current query
	if where == nil || where.GetFilterTyped() == nil {
		return p, nil
	}

	return applyPredicate(p, where.GetFilterTyped()).matchPredicateErrors(filtering)
}

func (p PromQLBuilder) ToQuery() (*prompb.Query, error) {
	pbQuery, err := remote.ToQuery(
		p.startTime,
		p.endTime,
		p.labelMatchers,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("to query: %w", err)
	}

	return pbQuery, nil
}

func applyPredicate(p PromQLBuilder, predicate *protos.TPredicate) PromQLBuilder {
	// Now we support only conjunction and comparison predicates
	switch pred := predicate.Payload.(type) {
	case *protos.TPredicate_Conjunction:
		for _, curPred := range pred.Conjunction.GetOperands() {
			p = applyPredicate(p, curPred)
		}
	case *protos.TPredicate_Comparison:
		return p.applyComparisonPredicate(predicate.GetComparison())
	default:
		p.predicateErrors = append(p.predicateErrors, fmt.Errorf("%w, type: %T", common.ErrUnimplementedPredicateType, pred))
	}

	return p
}

func (p PromQLBuilder) applyComparisonPredicate(c *protos.TPredicate_TComparison) PromQLBuilder {
	lv, rv, op := c.GetLeftValue(), c.GetRightValue(), c.GetOperation()
	if op == protos.TPredicate_TComparison_COMPARISON_OPERATION_UNSPECIFIED || lv == nil || rv == nil {
		p.predicateErrors = append(p.predicateErrors, fmt.Errorf("get comparison predicate: %w", common.ErrInvalidRequest))
		return p
	}

	// Now we support only WHERE <column> <operation> <value>
	// For example: `WHERE timestamp >= Timestamp("2025-03-17T16:00:00Z")`
	switch {
	case lv.GetColumn() != "" && rv.GetTypedValue() != nil:
		switch lv.GetColumn() {
		// If column is `timestamp` we must change `startTime` and `endTime` params
		case timestampColumn:
			return p.applyTimestampExpr(op, rv.GetTypedValue())
		// If column is `value`, we can`t push down this predicate, because remote read client provide only
		// `from`/`to` time options and label matchers
		case valueColumn:
			return p
		// Other columns is a strings, that represent prometheus labels
		default:
			return p.applyStringExpr(op, lv.GetColumn(), rv.GetTypedValue())
		}
	default:
		p.predicateErrors = append(p.predicateErrors, fmt.Errorf("apply comparison predicate: %w", common.ErrUnsupportedExpression))
		return p
	}
}

func (p PromQLBuilder) applyTimestampExpr(op protos.TPredicate_TComparison_EOperation, value *Ydb.TypedValue) PromQLBuilder {
	// Now we support only `Ydb.Type_TIMESTAMP` type
	if value.Type.GetTypeId() != Ydb.Type_TIMESTAMP {
		p.predicateErrors = append(p.predicateErrors, fmt.Errorf(
			"get type for timestamp expression: %w, type %s",
			common.ErrDataTypeNotSupported, value.Type.GetTypeId().String()))

		return p
	}

	if value.GetValue() == nil ||
		value.GetValue().GetUint64Value() == 0 {
		p.predicateErrors = append(p.predicateErrors, fmt.Errorf("get timestamp value: %w", common.ErrInvalidRequest))
		return p
	}

	// Because YDB convert timestamp using `time.UnixMicro`
	promTime := toPromTime(time.UnixMicro(int64(value.GetValue().GetUint64Value())))

	// We use +1 or -1 for operators where the operands are not assumed to be equal.
	// These values equivalent one nanosecond.
	switch op {
	case protos.TPredicate_TComparison_L:
		p.endTime = promTime - 1
	case protos.TPredicate_TComparison_LE:
		p.endTime = promTime
	case protos.TPredicate_TComparison_G:
		p.startTime = promTime + 1
	case protos.TPredicate_TComparison_GE:
		p.startTime = promTime
	case protos.TPredicate_TComparison_EQ:
		p.startTime = promTime
		p.endTime = promTime
	default:
		p.predicateErrors = append(p.predicateErrors, fmt.Errorf(
			"apply timestamp expression: %w, type: %s",
			common.ErrUnimplementedOperation, op.String()))
	}

	return p
}

func (p PromQLBuilder) applyStringExpr(op protos.TPredicate_TComparison_EOperation, column string, value *Ydb.TypedValue) PromQLBuilder {
	if value.Type.GetTypeId() != Ydb.Type_STRING {
		p.predicateErrors = append(p.predicateErrors, fmt.Errorf(
			"get type for string expression: %w, type %s",
			common.ErrDataTypeNotSupported, value.Type.GetTypeId().String()))

		return p
	}

	if value.GetValue() == nil {
		p.predicateErrors = append(p.predicateErrors, fmt.Errorf("get string value: %w", common.ErrInvalidRequest))
		return p
	}

	var matchType labels.MatchType

	switch op {
	case protos.TPredicate_TComparison_EQ:
		matchType = labels.MatchEqual
	case protos.TPredicate_TComparison_NE:
		matchType = labels.MatchNotEqual
	default:
		p.predicateErrors = append(p.predicateErrors, fmt.Errorf(
			"apply string expression: %w, type: %s",
			common.ErrUnimplementedOperation, op.String()))

		return p
	}

	p.labelMatchers = append(p.labelMatchers, &labels.Matcher{
		Type:  matchType,
		Name:  column,
		Value: value.GetValue().GetTextValue(),
	})

	return p
}

func (p PromQLBuilder) matchPredicateErrors(filtering protos.TReadSplitsRequest_EFiltering) (PromQLBuilder, error) {
	switch filtering {
	case protos.TReadSplitsRequest_FILTERING_UNSPECIFIED,
		protos.TReadSplitsRequest_FILTERING_OPTIONAL:
		var lastFatalErr error

		for _, err := range p.predicateErrors {
			if acceptableErrors.Match(err) {
				p.logger.Info("considering pushdown error as acceptable", zap.Error(err))
				continue
			}

			lastFatalErr = err
		}

		if lastFatalErr != nil {
			return PromQLBuilder{}, lastFatalErr
		}

		return p, nil
	case protos.TReadSplitsRequest_FILTERING_MANDATORY:
		var lastErr error
		if len(p.predicateErrors) > 0 {
			lastErr = p.predicateErrors[len(p.predicateErrors)-1]
		}

		return p, lastErr
	default:
		return PromQLBuilder{}, fmt.Errorf("unknown filtering mode: %d", filtering)
	}
}
