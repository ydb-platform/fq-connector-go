package prometheus

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/api/service/protos"
)

const (
	prometheusNameLabel = "__name__"
)

type PromQLBuilder struct {
	labelMatchers []*labels.Matcher
	startTime     int64
	endTime       int64
}

func NewPromQLBuilder() PromQLBuilder {
	return PromQLBuilder{
		// Because we will at least add the `from` expression.
		labelMatchers: make([]*labels.Matcher, 0, 1),
		// By default, we collect all metrics before `startTime`
		endTime: toPromTime(time.Now().Add(time.Hour)),
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

func (p PromQLBuilder) WithYdbWhere(whereArr []*protos.TSelect_TWhere) PromQLBuilder {
	for _, where := range whereArr {
		if where == nil || where.GetFilterTyped() == nil {
			continue
		}

		// Now we support only comparison predicate
		switch predicate := where.GetFilterTyped(); {
		case predicate.GetComparison() != nil:
			p = p.applyComparisonPredicate(predicate.GetComparison())
		default:
		}
	}

	return p
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

func (p PromQLBuilder) applyComparisonPredicate(c *protos.TPredicate_TComparison) PromQLBuilder {
	if c == nil ||
		c.GetOperation() == protos.TPredicate_TComparison_COMPARISON_OPERATION_UNSPECIFIED ||
		c.GetLeftValue() == nil ||
		c.GetRightValue() == nil {
		return p
	}

	// Now we support only WHERE <column> <operation> <value>
	// For example: `WHERE timestamp >= Timestamp("2025-03-17T16:00:00Z")`
	switch lv, rv, op := c.GetLeftValue(), c.GetRightValue(), c.GetOperation(); {
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
		return p
	}
}

func (p PromQLBuilder) applyTimestampExpr(op protos.TPredicate_TComparison_EOperation, value *Ydb.TypedValue) PromQLBuilder {
	// Now we support only `Ydb.Type_TIMESTAMP` type
	if value == nil ||
		value.Type.GetTypeId() != Ydb.Type_TIMESTAMP ||
		value.GetValue() == nil ||
		value.GetValue().GetUint64Value() == 0 {
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
	}

	return p
}

func (p PromQLBuilder) applyStringExpr(op protos.TPredicate_TComparison_EOperation, column string, value *Ydb.TypedValue) PromQLBuilder {
	if value == nil ||
		value.Type.GetTypeId() != Ydb.Type_STRING ||
		value.GetValue() == nil {
		return p
	}

	var matchType labels.MatchType

	switch op {
	case protos.TPredicate_TComparison_EQ:
		matchType = labels.MatchEqual
	case protos.TPredicate_TComparison_NE:
		matchType = labels.MatchNotEqual
	default:
		return p
	}

	p.labelMatchers = append(p.labelMatchers, &labels.Matcher{
		Type:  matchType,
		Name:  column,
		Value: value.GetValue().GetTextValue(),
	})

	return p
}
