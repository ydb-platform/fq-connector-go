package mongodb

import (
	"encoding/hex"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func makeFilter(
	logger *zap.Logger,
	split *api_service_protos.TSplit,
	filtering api_service_protos.TReadSplitsRequest_EFiltering,
) (bson.D, *options.FindOptions, error) {
	opts := options.Find()

	what := split.Select.What
	if what != nil {
		projection := bson.D{}
		for _, item := range what.GetItems() {
			projection = append(projection, bson.E{item.GetColumn().Name, 1})
		}

		opts.SetProjection(projection)
	}

	limit := split.Select.Limit
	if limit != nil {
		opts.SetSkip(int64(limit.Offset))
		opts.SetLimit(int64(limit.Limit))
		// opts.SetSort() ORDER BY :(
	}

	where := split.Select.Where
	filterTyped := where.GetFilterTyped()
	if filterTyped == nil {
		logger.Warn("handling nil filter")

		return bson.D{}, opts, nil
	}

	filter, err := makePredicateFilter(filterTyped)
	if err != nil {
		switch filtering {
		case api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY:
			return nil, nil, fmt.Errorf("encountered an error making a filter: %w", err)
		case api_service_protos.TReadSplitsRequest_FILTERING_UNSPECIFIED,
			api_service_protos.TReadSplitsRequest_FILTERING_OPTIONAL:
			if common.AcceptableErrors.Match(err) {
				logger.Info("considering pushdown error as acceptable", zap.Error(err))
				return filter, opts, nil
			}
			return nil, nil, fmt.Errorf("encountered an error making a filter: %w", err)
		default:
			return nil, nil, fmt.Errorf("unknown filtering mode: %d", filtering)
		}
	}

	return filter, opts, nil
}

func makePredicateFilter(
	predicate *api_service_protos.TPredicate,
) (bson.D, error) {
	var (
		result bson.D
		err    error
	)

	switch p := predicate.Payload.(type) {
	case *api_service_protos.TPredicate_IsNull:
		result, err = getIsNullFilter(p.IsNull.GetValue())
		if err != nil {
			return result, fmt.Errorf("get IsNull filter: %w", err)
		}
	case *api_service_protos.TPredicate_IsNotNull:
		result, err = getIsNotNullFilter(p.IsNotNull.GetValue())
		if err != nil {
			return result, fmt.Errorf("get IsNotNull filter: %w", err)
		}
	case *api_service_protos.TPredicate_Negation:
		result, err = getNegationFilter(p.Negation)
		if err != nil {
			return result, fmt.Errorf("get Negation filter: %w", err)
		}
	case *api_service_protos.TPredicate_Conjunction:
		result, err = getConjunctionFilter(p.Conjunction)
		if err != nil {
			return result, fmt.Errorf("get Conjunction filter: %w", err)
		}
	case *api_service_protos.TPredicate_Disjunction:
		result, err = getDisjunctionFilter(p.Disjunction)
		if err != nil {
			return result, fmt.Errorf("get Disjunction filter: %w", err)
		}
	case *api_service_protos.TPredicate_Comparison:
		result, err = getComparisonFilter(p.Comparison)
		if err != nil {
			return result, fmt.Errorf("get Comparison filter: %w", err)
		}
	case *api_service_protos.TPredicate_BoolExpression:
		result, err = getBooleanFilter(p.BoolExpression)
		if err != nil {
			return result, fmt.Errorf("get BoolExpression filter: %w", err)
		}
	case *api_service_protos.TPredicate_In:
		result, err = getInSetFilter(p.In)
		if err != nil {
			return result, fmt.Errorf("get InSet filter: %w", err)
		}
	case *api_service_protos.TPredicate_Between:
		result, err = getBetweenFilter(p.Between)
		if err != nil {
			return result, fmt.Errorf("get Between filter: %w", err)
		}
	case *api_service_protos.TPredicate_Regexp:
		result, err = getRegexFilter(p.Regexp)
		if err != nil {
			return result, fmt.Errorf("get If filter: %w", err)
		}
	default:
		return nil, fmt.Errorf("%w, type: %T", common.ErrUnimplementedPredicateType, p)
	}

	return result, nil
}

func getNegationFilter(
	negation *api_service_protos.TPredicate_TNegation,
) (bson.D, error) {
	operand, err := makePredicateFilter(negation.Operand)
	if err != nil {
		return nil, err
	}

	return bson.D{{"$nor", bson.A{operand}}}, nil
}

func getBooleanFilter(
	boolExpression *api_service_protos.TPredicate_TBoolExpression,
) (bson.D, error) {
	expr, err := formatExpression(boolExpression.Value)
	if err != nil {
		return nil, err
	}

	return bson.D{{"$expr", bson.D{{"$eq", bson.A{expr, true}}}}}, nil
}

func getConjunctionFilter(
	conjunction *api_service_protos.TPredicate_TConjunction,
) (bson.D, error) {
	operands := make([]bson.D, 0, len(conjunction.Operands))
	for _, op := range conjunction.Operands {
		operand, err := makePredicateFilter(op)
		if err != nil {
			return nil, err
		}
		operands = append(operands, operand)
	}

	return bson.D{{"$and", operands}}, nil
}

func getDisjunctionFilter(
	disjunction *api_service_protos.TPredicate_TDisjunction,
) (bson.D, error) {

	operands := make([]bson.D, 0, len(disjunction.Operands))
	for _, op := range disjunction.Operands {
		operand, err := makePredicateFilter(op)
		if err != nil {
			return nil, err
		}
		operands = append(operands, operand)
	}

	return bson.D{{"$or", operands}}, nil
}

func getIsNotNullFilter(expression *api_service_protos.TExpression) (bson.D, error) {
	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return bson.D{{e.Column, bson.D{{"$ne", nil}}}}, nil
	default:
		return nil, common.ErrUnimplementedPredicateType
	}
}

func getIsNullFilter(expression *api_service_protos.TExpression) (bson.D, error) {
	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return bson.D{
			{"$or", bson.A{
				bson.D{{e.Column, bson.D{{"$exists", false}}}},
				bson.D{{e.Column, bson.D{{"$eq", nil}}}},
			}},
		}, nil
	default:
		return nil, common.ErrUnimplementedPredicateType
	}
}

func getComparisonFilter(comparison *api_service_protos.TPredicate_TComparison) (bson.D, error) {
	var operation string

	switch op := comparison.Operation; op {
	case api_service_protos.TPredicate_TComparison_L:
		operation = "$lt"
	case api_service_protos.TPredicate_TComparison_LE:
		operation = "$lte"
	case api_service_protos.TPredicate_TComparison_EQ:
		operation = "$eq"
	case api_service_protos.TPredicate_TComparison_NE:
		operation = "$ne"
	case api_service_protos.TPredicate_TComparison_GE:
		operation = "$gte"
	case api_service_protos.TPredicate_TComparison_G:
		operation = "$gt"
	default:
		return nil, fmt.Errorf("%w, op: %d", common.ErrUnimplementedOperation, op)
	}

	left, err := formatExpression(comparison.LeftValue)
	if err != nil {
		return nil, fmt.Errorf("format left expression: %v: %w", comparison.LeftValue, err)
	}

	right, err := formatExpression(comparison.RightValue)
	if err != nil {
		return nil, fmt.Errorf("format right expression: %v: %w", comparison.RightValue, err)
	}

	return bson.D{{"$expr", bson.D{{operation, bson.A{left, right}}}}}, nil
}

func getInSetFilter(
	in *api_service_protos.TPredicate_TIn,
) (bson.D, error) {
	var fieldName string
	switch e := in.Value.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		fieldName = e.Column
	default:
		return nil, common.ErrUnimplementedPredicateType
	}

	inSet := make([]any, 0, len(in.Set))
	for _, e := range in.Set {
		expr, err := formatExpression(e)
		if err != nil {
			return nil, err
		}

		inSet = append(inSet, expr)
	}

	return bson.D{{fieldName, bson.D{{"$in", inSet}}}}, nil
}

func getBetweenFilter(
	between *api_service_protos.TPredicate_TBetween,
) (bson.D, error) {
	var fieldName string

	switch e := between.Value.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		fieldName = e.Column
	default:
		return nil, common.ErrUnimplementedPredicateType
	}

	least, err := formatExpression(between.Least)
	if err != nil {
		return nil, fmt.Errorf("format least expression: %v: %w", between.Least, err)
	}

	greatest, err := formatExpression(between.Greatest)
	if err != nil {
		return nil, fmt.Errorf("format greatest expression: %v: %w", between.Greatest, err)
	}

	return bson.D{{fieldName,
		bson.D{
			{"$gte", least},
			{"$lte", greatest},
		},
	}}, nil
}

func getRegexFilter(
	regex *api_service_protos.TPredicate_TRegexp,
) (bson.D, error) {
	var fieldName string

	switch e := regex.Value.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		fieldName = e.Column
	default:
		return nil, common.ErrUnimplementedExpression
	}

	pattern, err := formatExpression(regex.Pattern)
	if err != nil {
		return nil, fmt.Errorf("format regex pattern expression: %v: %w", regex.Pattern, err)
	}

	return bson.D{{fieldName, bson.D{{"$regex", pattern}}}}, nil
}

func formatExpression(expression *api_service_protos.TExpression) (any, error) {
	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return fmt.Sprintf("$%v", e.Column), nil
	case *api_service_protos.TExpression_TypedValue:
		return formatTypedValue(e.TypedValue)
	case *api_service_protos.TExpression_Null:
		return nil, nil
	case *api_service_protos.TExpression_Coalesce:
		return formatCoalesce(e.Coalesce)
	default:
		return nil, fmt.Errorf("%w, type: %T", common.ErrUnimplementedExpression, e)
	}
}

func formatTypedValue(expr *Ydb.TypedValue) (any, error) {
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
	case *Ydb.Value_Uint32Value:
		value = t.Uint32Value
	case *Ydb.Value_Int64Value:
		value = t.Int64Value
	case *Ydb.Value_Uint64Value:
		value = t.Uint64Value
	case *Ydb.Value_FloatValue:
		value = t.FloatValue
	case *Ydb.Value_DoubleValue:
		value = t.DoubleValue
	case *Ydb.Value_BytesValue:
		value = t.BytesValue
	case *Ydb.Value_TextValue:
		value = t.TextValue
	default:
		return nil, fmt.Errorf("%w, type: %T", common.ErrUnimplementedTypedValue, t)
	}

	value, err := tryFormatObjectId(ydbType, value)
	if err != nil {
		return nil, fmt.Errorf("%w %w", err, common.ErrUnimplementedTypedValue)
	}

	return value, nil
}

func formatCoalesce(expr *api_service_protos.TExpression_TCoalesce) (any, error) {
	operands := make([]any, 0, len(expr.Operands))
	for _, opExpr := range expr.Operands {
		op, err := formatExpression(opExpr)
		if err != nil {
			return nil, fmt.Errorf("error formatting coalesce expression: %w", err)
		}

		operands = append(operands, op)
	}

	return bson.D{{"$ifNull", operands}}, nil
}

func tryFormatObjectId(exprType *Ydb.Type, value any) (any, error) {
	for exprType.GetOptionalType() != nil {
		exprType = exprType.GetOptionalType().GetItem()
	}

	switch t := exprType.Type.(type) {
	case *Ydb.Type_TaggedType:
		if !common.TypesEqual(exprType, objectIdType) {
			return nil, fmt.Errorf("unknown Tagged type: %T", exprType)
		}

		var hexString string

		switch b := value.(type) {
		case []byte:
			hexString = hex.EncodeToString(b)
		case string:
			hexString = b
		default:
			return nil, fmt.Errorf("wrong value of TypedValue for ObjectId: %v", value)
		}

		v, err := primitive.ObjectIDFromHex(hexString)
		if err != nil {
			return nil, fmt.Errorf("failed to construct ObjectId from %s %v: %w", hexString, value, err)
		}

		return v, nil
	case *Ydb.Type_TypeId:
		switch t.TypeId {
		case Ydb.Type_BOOL, Ydb.Type_INT8, Ydb.Type_UINT8, Ydb.Type_INT16,
			Ydb.Type_UINT16, Ydb.Type_INT32, Ydb.Type_UINT32, Ydb.Type_INT64,
			Ydb.Type_UINT64, Ydb.Type_FLOAT, Ydb.Type_DOUBLE, Ydb.Type_STRING, Ydb.Type_UTF8:
			return value, nil
		default:
			return nil, fmt.Errorf("unsupported type %T for typed value", t)
		}
	default:
		return nil, fmt.Errorf("unsupported type %T for typed value", t)
	}
}
