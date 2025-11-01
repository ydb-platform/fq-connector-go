package opensearch

import (
	"bytes"
	"encoding/json"
	"errors"
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

// buildSearchQuery constructs OpenSearch query with support for:
//   - Projection: specify exact fields to return from documents.
//     Format: []string{"field", "nested.field", "deep.nested.field"}
//     Examples:
//     ["user.name"] - return only 'name' from 'user' object
//     ["meta.tags"] - return 'tags' array from 'meta' object
//     Note:
//   - OpenSearch requires full path to nested fields
//   - Wildcards (e.g., "user.*") are NOT supported here
//   - Invalid fields will be silently ignored by OpenSearch
//   - Predicate pushdown: filter documents at source
//   - Pagination: control batch size via scroll API
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
		return nil, nil, errors.New("not specified columns to query in Select.What")
	}

	// TODO (Test for top to bottom struct projection)
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

		filter, err = qb.makePredicateFilter(where.FilterTyped, true)
		if err != nil {
			switch filtering {
			case api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY:
				return nil, nil, fmt.Errorf("make predicate filter: %w", err)
			case api_service_protos.TReadSplitsRequest_FILTERING_OPTIONAL:
				if !common.OptionalFilteringAllowedErrors.Match(err) {
					return nil, nil, fmt.Errorf("encountered an error making a filter: %w", err)
				}

				qb.logger.Warn("considering pushdown error as acceptable", zap.Error(err))
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

//nolint:funlen,gocyclo
func (qb *queryBuilder) makePredicateFilter(
	predicate *api_service_protos.TPredicate,
	topLevel bool,
) (map[string]any, error) {
	switch p := predicate.Payload.(type) {
	case *api_service_protos.TPredicate_IsNull:
		filter, err := qb.makeIsNullFilter(p.IsNull.GetValue())
		if err != nil {
			return nil, fmt.Errorf("make is null filter: %w", err)
		}

		return filter, nil
	case *api_service_protos.TPredicate_IsNotNull:
		filter, err := qb.makeIsNotNullFilter(p.IsNotNull.GetValue())
		if err != nil {
			return nil, fmt.Errorf("make is not null filter: %w", err)
		}

		return filter, nil
	case *api_service_protos.TPredicate_Negation:
		filter, err := qb.makeNegationFilter(p.Negation)
		if err != nil {
			return nil, fmt.Errorf("make negation filter: %w", err)
		}

		return filter, nil
	case *api_service_protos.TPredicate_Conjunction:
		filter, err := qb.makeConjunctionFilter(p.Conjunction, topLevel)
		if err != nil {
			return nil, fmt.Errorf("make conjunction filter: %w", err)
		}

		return filter, nil
	case *api_service_protos.TPredicate_Disjunction:
		filter, err := qb.makeDisjunctionFilter(p.Disjunction)
		if err != nil {
			return nil, fmt.Errorf("make disjunction filter: %w", err)
		}

		return filter, nil
	case *api_service_protos.TPredicate_Comparison:
		filter, err := qb.makeComparisonFilter(p.Comparison)
		if err != nil {
			return nil, fmt.Errorf("make comparison filter: %w", err)
		}

		return filter, nil
	case *api_service_protos.TPredicate_BoolExpression:
		filter, err := qb.makeBooleanFilter(p.BoolExpression)
		if err != nil {
			return nil, fmt.Errorf("make bool expression filter: %w", err)
		}

		return filter, nil
	case *api_service_protos.TPredicate_In:
		filter, err := qb.makeInSetFilter(p.In)
		if err != nil {
			return nil, fmt.Errorf("make in set filter: %w", err)
		}

		return filter, nil
	case *api_service_protos.TPredicate_Between:
		filter, err := qb.makeBetweenFilter(p.Between)
		if err != nil {
			return nil, fmt.Errorf("make between filter: %w", err)
		}

		return filter, nil
	case *api_service_protos.TPredicate_Regexp:
		filter, err := qb.makeRegexFilter(p.Regexp)
		if err != nil {
			return nil, fmt.Errorf("make regex filter: %w", err)
		}

		return filter, nil
	default:
		return nil, fmt.Errorf("%w: %T", common.ErrUnimplementedPredicateType, p)
	}
}
func (qb *queryBuilder) makeNegationFilter(negation *api_service_protos.TPredicate_TNegation) (map[string]any, error) {
	filter, err := qb.makePredicateFilter(negation.Operand, false)
	if err != nil {
		return nil, fmt.Errorf("make predicate filter: %w", err)
	}

	return map[string]any{
		"bool": map[string]any{
			"must_not": []any{filter},
		},
	}, nil
}

func (qb *queryBuilder) makeBooleanFilter(boolExpr *api_service_protos.TPredicate_TBoolExpression) (map[string]any, error) {
	field, err := qb.getFieldName(boolExpr.Value)
	if err != nil {
		return nil, fmt.Errorf("get field name: %w", err)
	}

	return map[string]any{
		"bool": map[string]any{
			"must": []any{
				map[string]any{
					"term": map[string]any{
						field: true,
					},
				},
			},
		},
	}, nil
}

func (qb *queryBuilder) makeConjunctionFilter(
	conjunction *api_service_protos.TPredicate_TConjunction,
	topLevel bool,
) (map[string]any, error) {
	var (
		must []map[string]any
		errs []error
	)

	for _, op := range conjunction.Operands {
		filter, err := qb.makePredicateFilter(op, false)
		if err != nil {
			if topLevel {
				errs = append(errs, fmt.Errorf("operand error: %w", err))

				continue
			}

			return nil, fmt.Errorf("make predicate filter: %w", err)
		}

		must = append(must, filter)
	}

	if topLevel && len(errs) > 0 {
		return nil, fmt.Errorf("%d errors in conjunction: %v", len(errs), errs)
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
		filter, err := qb.makePredicateFilter(op, false)
		if err != nil {
			return nil, fmt.Errorf("make predicate filter: %w", err)
		}

		should = append(should, filter)
	}

	// Logical or operator. The results must match at least one of the queries.
	// Matching more should clauses increases the document’s relevance score.
	// You can set the minimum number of queries that must match using the minimum_should_match parameter.
	// If a query contains a must or filter clause, the default minimum_should_match value is 0.
	// Otherwise, the default minimum_should_match value is 1.
	// https://docs.opensearch.org/docs/latest/query-dsl/compound/bool/
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
		return nil, fmt.Errorf("get field name: %w", err)
	}

	return map[string]any{
		"bool": map[string]any{
			"must_not": []any{
				map[string]any{
					"exists": map[string]any{
						"field": field,
					},
				},
			},
		},
	}, nil
}

func (qb *queryBuilder) makeIsNotNullFilter(expr *api_service_protos.TExpression) (map[string]any, error) {
	field, err := qb.getFieldName(expr)
	if err != nil {
		return nil, fmt.Errorf("get field name: %w", err)
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
		return nil, fmt.Errorf("get field name: %w", err)
	}

	value, err := qb.makeExpressionValue(comparison.RightValue)
	if err != nil {
		return nil, fmt.Errorf("make expression value: %w", err)
	}

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
				"must_not": []any{
					map[string]any{
						"term": map[string]any{
							field: value,
						},
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
		return nil, fmt.Errorf("get field name: %w", err)
	}

	var values []any

	for _, item := range in.Set {
		value, err := qb.makeExpressionValue(item)
		if err != nil {
			return nil, fmt.Errorf("make expression value: %w", err)
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
		return nil, fmt.Errorf("get field name: %w", err)
	}

	mn, err := qb.makeExpressionValue(between.Least)
	if err != nil {
		return nil, fmt.Errorf("make expression value: %w", err)
	}

	mx, err := qb.makeExpressionValue(between.Greatest)
	if err != nil {
		return nil, fmt.Errorf("make expression value: %w", err)
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
		return nil, fmt.Errorf("get field name: %w", err)
	}

	pattern, err := qb.makeExpressionValue(regex.Pattern)
	if err != nil {
		return nil, fmt.Errorf("make expression value: %w", err)
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

	if v == nil {
		return nil, errors.New("typed value container is nil")
	}

	if v.Value == nil {
		return nil, fmt.Errorf("typed value content is nil (container type: %T)", v)
	}

	if ydbType == nil {
		return nil, fmt.Errorf("YDB type descriptor is nil (value container: %+v)", v)
	}

	var value any

	switch t := v.Value.(type) {
	case *Ydb.Value_BoolValue:
		value = t.BoolValue
	case *Ydb.Value_Int32Value:
		value = t.Int32Value
	case *Ydb.Value_Int64Value:
		value = t.Int64Value
	case *Ydb.Value_Uint32Value:
		value = t.Uint32Value
	case *Ydb.Value_Uint64Value:
		value = t.Uint64Value
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
		case Ydb.Type_BOOL, Ydb.Type_UINT32, Ydb.Type_UINT64, Ydb.Type_INT32,
			Ydb.Type_INT64, Ydb.Type_FLOAT, Ydb.Type_DOUBLE,
			Ydb.Type_STRING, Ydb.Type_UTF8:
			return value, nil
		default:
			return nil, fmt.Errorf("unsupported type %T for typed value", t)
		}
	default:
		return nil, fmt.Errorf("unsupported type %T for typed value", t)
	}
}
