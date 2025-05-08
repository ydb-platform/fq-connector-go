package opensearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

type queryBuilder struct {
	logger *zap.Logger
}

func newQueryBuilder(logger *zap.Logger) *queryBuilder {
	return &queryBuilder{logger: logger}
}

func (qb *queryBuilder) buildSearchQuery(
	split *api_service_protos.TSplit,
	filtering api_service_protos.TReadSplitsRequest_EFiltering,
	batchSize uint64,
	scrollTimeout time.Duration,
) (io.Reader, *opensearchapi.SearchParams, error) {
	params := &opensearchapi.SearchParams{
		Scroll: scrollTimeout,
	}

	what := split.Select.GetWhat()
	if what == nil {
		return nil, nil, fmt.Errorf("not specified columns to query in Select.What")
	}

	var projection []string
	for _, item := range what.GetItems() {
		projection = append(projection, item.GetColumn().Name)
	}

	query := map[string]any{
		"size":    batchSize,
		"_source": projection,
	}

	limit := split.Select.GetLimit()
	if limit != nil {
		from := int(limit.Offset)
		size := int(limit.Limit)

		params.From = &from
		params.Size = &size
	}

	where := split.Select.GetWhere()

	var filter map[string]any

	if where != nil && where.FilterTyped != nil {
		var err error

		filter, err = qb.makePredicateFilter(where.FilterTyped)
		if err != nil {
			switch filtering {
			case api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY:
				return nil, nil, fmt.Errorf("make predicate filter: %w", err)
			case api_service_protos.TReadSplitsRequest_FILTERING_UNSPECIFIED,
				api_service_protos.TReadSplitsRequest_FILTERING_OPTIONAL:
				if common.OptionalFilteringAllowedErrors.Match(err) {
					qb.logger.Warn("considering pushdown error as acceptable", zap.Error(err))
				} else {
					return nil, nil, fmt.Errorf("encountered an error making a filter: %w", err)
				}
			default:
				return nil, nil, fmt.Errorf("unknown filtering mode: %d", filtering)
			}
		} else {
			query["query"] = filter
		}
	} else {
		query["query"] = map[string]any{
			"match_all": make(map[string]any),
		}
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, nil, fmt.Errorf("encode query: %w", err)
	}

	return &buf, params, nil
}

func (qb *queryBuilder) makePredicateFilter(predicate *api_service_protos.TPredicate) (map[string]any, error) {
	switch p := predicate.Payload.(type) {
	case *api_service_protos.TPredicate_IsNull:
		return qb.makeIsNullFilter(p.IsNull.GetValue())
	case *api_service_protos.TPredicate_IsNotNull:
		return qb.makeIsNotNullFilter(p.IsNotNull.GetValue())
	case *api_service_protos.TPredicate_Negation:
		return qb.makeNegationFilter(p.Negation)
	case *api_service_protos.TPredicate_Conjunction:
		return qb.makeConjunctionFilter(p.Conjunction)
	case *api_service_protos.TPredicate_Disjunction:
		return qb.makeDisjunctionFilter(p.Disjunction)
	case *api_service_protos.TPredicate_Comparison:
		return qb.makeComparisonFilter(p.Comparison)
	case *api_service_protos.TPredicate_BoolExpression:
		return qb.makeBooleanFilter(p.BoolExpression)
	case *api_service_protos.TPredicate_In:
		return qb.makeInSetFilter(p.In)
	case *api_service_protos.TPredicate_Between:
		return qb.makeBetweenFilter(p.Between)
	case *api_service_protos.TPredicate_Regexp:
		return qb.makeRegexFilter(p.Regexp)
	default:
		return nil, fmt.Errorf("%w: %T", common.ErrUnimplementedPredicateType, p)
	}
}

func (qb *queryBuilder) makeNegationFilter(negation *api_service_protos.TPredicate_TNegation) (map[string]any, error) {
	if cmp, ok := negation.Operand.Payload.(*api_service_protos.TPredicate_Comparison); ok {
		invertedOp, err := qb.invertComparisonOperation(cmp.Comparison.Operation)
		if err != nil {
			return nil, fmt.Errorf("invert comparison operation: %w", err)
		}

		return qb.makeComparisonFilter(&api_service_protos.TPredicate_TComparison{
			LeftValue:  cmp.Comparison.LeftValue,
			Operation:  invertedOp,
			RightValue: cmp.Comparison.RightValue,
		})
	}

	filter, err := qb.makePredicateFilter(negation.Operand)
	if err != nil {
		return nil, fmt.Errorf("make negation filter: %w", err)
	}

	return map[string]any{
		"bool": map[string]any{
			"must_not": filter,
		},
	}, nil
}

func (*queryBuilder) invertComparisonOperation(
	op api_service_protos.TPredicate_TComparison_EOperation,
) (api_service_protos.TPredicate_TComparison_EOperation, error) {
	switch op {
	case api_service_protos.TPredicate_TComparison_EQ:
		return api_service_protos.TPredicate_TComparison_NE, nil
	case api_service_protos.TPredicate_TComparison_NE:
		return api_service_protos.TPredicate_TComparison_EQ, nil
	case api_service_protos.TPredicate_TComparison_L:
		return api_service_protos.TPredicate_TComparison_GE, nil
	case api_service_protos.TPredicate_TComparison_LE:
		return api_service_protos.TPredicate_TComparison_G, nil
	case api_service_protos.TPredicate_TComparison_G:
		return api_service_protos.TPredicate_TComparison_LE, nil
	case api_service_protos.TPredicate_TComparison_GE:
		return api_service_protos.TPredicate_TComparison_L, nil
	default:
		return 0, fmt.Errorf("cannot invert operation %v", op)
	}
}

func (qb *queryBuilder) makeBooleanFilter(boolExpr *api_service_protos.TPredicate_TBoolExpression) (map[string]any, error) {
	field, err := qb.getFieldName(boolExpr.Value)
	if err != nil {
		return nil, fmt.Errorf("make boolean filter: %w", err)
	}

	return map[string]any{
		"bool": map[string]any{
			"must": map[string]any{
				"term": map[string]any{
					field: true,
				},
			},
		},
	}, nil
}

func (qb *queryBuilder) makeConjunctionFilter(conjunction *api_service_protos.TPredicate_TConjunction) (map[string]any, error) {
	var must []map[string]any

	for _, op := range conjunction.Operands {
		filter, err := qb.makePredicateFilter(op)
		if err != nil {
			return nil, fmt.Errorf("make conjunction filter: %w", err)
		}

		must = append(must, filter)
	}

	return map[string]any{
		"bool": map[string]any{
			"must": must,
		},
	}, nil
}

func (qb *queryBuilder) makeDisjunctionFilter(disjunction *api_service_protos.TPredicate_TDisjunction) (map[string]any, error) {
	var should []map[string]any

	for _, op := range disjunction.Operands {
		filter, err := qb.makePredicateFilter(op)
		if err != nil {
			return nil, fmt.Errorf("make disjunction filter: %w", err)
		}

		should = append(should, filter)
	}

	return map[string]any{
		"bool": map[string]any{
			"should":               should,
			"minimum_should_match": 1,
		},
	}, nil
}

func (qb *queryBuilder) makeIsNullFilter(expr *api_service_protos.TExpression) (map[string]any, error) {
	field, err := qb.getFieldName(expr)
	if err != nil {
		return nil, fmt.Errorf("make is null filter: %w", err)
	}

	return map[string]any{
		"bool": map[string]any{
			"must_not": map[string]any{
				"exists": map[string]any{
					"field": field,
				},
			},
		},
	}, nil
}

func (qb *queryBuilder) makeIsNotNullFilter(expr *api_service_protos.TExpression) (map[string]any, error) {
	field, err := qb.getFieldName(expr)
	if err != nil {
		return nil, fmt.Errorf("make is not null filter: %w", err)
	}

	return map[string]any{
		"exists": map[string]any{
			"field": field,
		},
	}, nil
}

func (qb *queryBuilder) makeComparisonFilter(comparison *api_service_protos.TPredicate_TComparison) (map[string]any, error) {
	field, err := qb.getFieldName(comparison.LeftValue)
	if err != nil {
		return nil, fmt.Errorf("make comparison filter: %w", err)
	}

	qb.logger.Debug("makeComparisonFilter", zap.Any("field", field))

	value, err := qb.makeExpressionValue(comparison.RightValue)
	if err != nil {
		return nil, fmt.Errorf("make comparison filter: %w", err)
	}

	qb.logger.Debug("makeComparisonFilter", zap.Any("value", value))

	switch comparison.Operation {
	case api_service_protos.TPredicate_TComparison_EQ:
		return map[string]any{
			"term": map[string]any{
				field: value,
			},
		}, nil
	case api_service_protos.TPredicate_TComparison_NE:
		return map[string]any{
			"bool": map[string]any{
				"must_not": map[string]any{
					"term": map[string]any{
						field: value,
					},
				},
			},
		}, nil
	case api_service_protos.TPredicate_TComparison_L:
		return map[string]any{
			"range": map[string]any{
				field: map[string]any{
					"lt": value,
				},
			},
		}, nil
	case api_service_protos.TPredicate_TComparison_LE:
		return map[string]any{
			"range": map[string]any{
				field: map[string]any{
					"lte": value,
				},
			},
		}, nil
	case api_service_protos.TPredicate_TComparison_G:
		return map[string]any{
			"range": map[string]any{
				field: map[string]any{
					"gt": value,
				},
			},
		}, nil
	case api_service_protos.TPredicate_TComparison_GE:
		return map[string]any{
			"range": map[string]any{
				field: map[string]any{
					"gte": value,
				},
			},
		}, nil
	case api_service_protos.TPredicate_TComparison_STARTS_WITH:
		return map[string]any{
			"prefix": map[string]any{
				fmt.Sprintf("%s.keyword", field): map[string]any{
					"value": value,
				},
			},
		}, nil
	case api_service_protos.TPredicate_TComparison_CONTAINS:
		return map[string]any{
			"wildcard": map[string]any{
				fmt.Sprintf("%s.keyword", field): map[string]any{
					"value": fmt.Sprintf("*%s*", value),
				},
			},
		}, nil
	case api_service_protos.TPredicate_TComparison_ENDS_WITH:
		return map[string]any{
			"wildcard": map[string]any{
				fmt.Sprintf("%s.keyword", field): map[string]any{
					"value": fmt.Sprintf("*%s", value),
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("%w: %d", common.ErrUnimplementedOperation, comparison.Operation)
	}
}

func (qb *queryBuilder) makeInSetFilter(in *api_service_protos.TPredicate_TIn) (map[string]any, error) {
	field, err := qb.getFieldName(in.Value)
	if err != nil {
		return nil, fmt.Errorf("make in set filter: %w", err)
	}

	var values []any

	for _, item := range in.Set {
		value, err := qb.makeExpressionValue(item)
		if err != nil {
			return nil, fmt.Errorf("make in set filter: %w", err)
		}

		values = append(values, value)
	}

	return map[string]any{
		"terms": map[string]any{
			field: values,
		},
	}, nil
}

func (qb *queryBuilder) makeBetweenFilter(between *api_service_protos.TPredicate_TBetween) (map[string]any, error) {
	field, err := qb.getFieldName(between.Value)
	if err != nil {
		return nil, fmt.Errorf("make between filter: %w", err)
	}

	mn, err := qb.makeExpressionValue(between.Least)
	if err != nil {
		return nil, fmt.Errorf("make between filter: %w", err)
	}

	mx, err := qb.makeExpressionValue(between.Greatest)
	if err != nil {
		return nil, fmt.Errorf("make between filter: %w", err)
	}

	return map[string]any{
		"range": map[string]any{
			field: map[string]any{
				"gte": mn,
				"lte": mx,
			},
		},
	}, nil
}

func (qb *queryBuilder) makeRegexFilter(regex *api_service_protos.TPredicate_TRegexp) (map[string]any, error) {
	field, err := qb.getFieldName(regex.Value)
	if err != nil {
		return nil, fmt.Errorf("make regex filter: %w", err)
	}

	pattern, err := qb.makeExpressionValue(regex.Pattern)
	if err != nil {
		return nil, fmt.Errorf("make regex filter: %w", err)
	}

	return map[string]any{
		"regexp": map[string]any{
			field: map[string]any{
				"value": pattern,
			},
		},
	}, nil
}

func (*queryBuilder) getFieldName(expr *api_service_protos.TExpression) (string, error) {
	switch e := expr.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return e.Column, nil
	default:
		return "", fmt.Errorf("%w: expected column name", common.ErrUnimplementedExpression)
	}
}

func (qb *queryBuilder) makeExpressionValue(expr *api_service_protos.TExpression) (any, error) {
	switch e := expr.Payload.(type) {
	case *api_service_protos.TExpression_TypedValue:
		return qb.makeTypedValue(e.TypedValue)
	case *api_service_protos.TExpression_Column:
		return e.Column, nil
	case *api_service_protos.TExpression_Null:
		return nil, nil
	default:
		return nil, fmt.Errorf("%w: %T", common.ErrUnimplementedExpression, e)
	}
}

func (qb *queryBuilder) makeTypedValue(expr *Ydb.TypedValue) (any, error) {
	v := expr.GetValue()
	ydbType := expr.GetType()

	if v == nil || v.Value == nil || ydbType == nil {
		return nil, fmt.Errorf("got %v of type %T as null trying to format Typed Value expression", v, v)
	}

	var value any
	switch t := v.Value.(type) {
	case *Ydb.Value_BoolValue:
		value = t.BoolValue
	case *Ydb.Value_Int32Value:
		value = t.Int32Value
	case *Ydb.Value_Int64Value:
		value = t.Int64Value
	case *Ydb.Value_FloatValue:
		value = t.FloatValue
	case *Ydb.Value_DoubleValue:
		value = t.DoubleValue
	case *Ydb.Value_TextValue:
		value = t.TextValue
	case *Ydb.Value_BytesValue:
		value = t.BytesValue
	default:
		return nil, fmt.Errorf("%w, type: %T", common.ErrUnimplementedTypedValue, t)
	}

	value, err := qb.formatValue(ydbType, value)
	if err != nil {
		return nil, fmt.Errorf("%w %w", err, common.ErrUnimplementedTypedValue)
	}

	return value, nil
}

func (*queryBuilder) formatValue(exprType *Ydb.Type, value any) (any, error) {
	for exprType.GetOptionalType() != nil {
		exprType = exprType.GetOptionalType().GetItem()
	}

	switch t := exprType.Type.(type) {
	case *Ydb.Type_TypeId:
		switch t.TypeId {
		case Ydb.Type_BOOL, Ydb.Type_INT32, Ydb.Type_INT64, Ydb.Type_FLOAT, Ydb.Type_DOUBLE, Ydb.Type_STRING, Ydb.Type_UTF8:
			return value, nil
		default:
			return nil, fmt.Errorf("unsupported type %T for typed value", t)
		}
	default:
		return nil, fmt.Errorf("unsupported type %T for typed value", t)
	}
}
