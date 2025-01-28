package utils

import (
	"fmt"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

func formatValue(formatter SQLFormatter, args *QueryArgs, value *Ydb.TypedValue) (string, *QueryArgs, error) {
	if value.Type.GetOptionalType() != nil {
		return formatOptionalValue(formatter, args, value)
	}

	return formatPrimitiveValue(formatter, args, value)
}

func formatPrimitiveValue(formatter SQLFormatter, args *QueryArgs, value *Ydb.TypedValue) (string, *QueryArgs, error) {
	switch v := value.Value.Value.(type) {
	case *Ydb.Value_BoolValue:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, v.BoolValue), nil
	case *Ydb.Value_Int32Value:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, v.Int32Value), nil
	case *Ydb.Value_Uint32Value:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, v.Uint32Value), nil
	case *Ydb.Value_Int64Value:
		switch value.Type.GetTypeId() {
		case Ydb.Type_INT64:
			return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, v.Int64Value), nil
		case Ydb.Type_TIMESTAMP:
			return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, time.UnixMicro(v.Int64Value)), nil
		default:
			return "", args, fmt.Errorf("unsupported type '%T': %w", v, common.ErrUnimplementedTypedValue)
		}
	case *Ydb.Value_Uint64Value:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, v.Uint64Value), nil
	case *Ydb.Value_FloatValue:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, v.FloatValue), nil
	case *Ydb.Value_DoubleValue:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, v.DoubleValue), nil
	case *Ydb.Value_BytesValue:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, v.BytesValue), nil
	case *Ydb.Value_TextValue:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, v.TextValue), nil
	case *Ydb.Value_NullFlagValue:
		placeholder, newArgs, err := formatNullFlagValue(formatter, args, value)
		if err != nil {
			return "", args, fmt.Errorf("format null flag value: %w", err)
		}

		return placeholder, newArgs, nil
	default:
		return "", args, fmt.Errorf("unsupported type '%T': %w", v, common.ErrUnimplementedTypedValue)
	}
}

func formatOptionalValue(formatter SQLFormatter, args *QueryArgs, value *Ydb.TypedValue) (string, *QueryArgs, error) {
	switch v := value.Value.Value.(type) {
	case *Ydb.Value_BoolValue:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, &v.BoolValue), nil
	case *Ydb.Value_Int32Value:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, &v.Int32Value), nil
	case *Ydb.Value_Uint32Value:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, &v.Uint32Value), nil
	case *Ydb.Value_Int64Value:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, &v.Int64Value), nil
	case *Ydb.Value_Uint64Value:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, &v.Uint64Value), nil
	case *Ydb.Value_FloatValue:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, &v.FloatValue), nil
	case *Ydb.Value_DoubleValue:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, &v.DoubleValue), nil
	case *Ydb.Value_BytesValue:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, &v.BytesValue), nil
	case *Ydb.Value_TextValue:
		return formatter.GetPlaceholder(args.Count()), args.AddTyped(value.Type, &v.TextValue), nil
	case *Ydb.Value_NullFlagValue:
		placeholder, newArgs, err := formatNullFlagValue(formatter, args, value)
		if err != nil {
			return "", args, fmt.Errorf("format null flag value: %w", err)
		}

		return placeholder, newArgs, nil
	default:
		return "", args, fmt.Errorf("unsupported type '%T': %w", v, common.ErrUnimplementedTypedValue)
	}
}

func addTypedNull[ACCEPTOR_TYPE any](
	formatter SQLFormatter,
	args *QueryArgs,
	ydbType *Ydb.Type,
) (string, *QueryArgs, error) {
	return formatter.GetPlaceholder(args.Count()), args.AddTyped(ydbType, (*ACCEPTOR_TYPE)(nil)), nil
}

func formatNullFlagValue(formatter SQLFormatter, args *QueryArgs, value *Ydb.TypedValue) (string, *QueryArgs, error) {
	optType, ok := value.Type.GetType().(*Ydb.Type_OptionalType)
	if !ok {
		return "", args, fmt.Errorf(
			"null flag values must be optionally typed, got type '%T' instead: %w",
			value.Type.GetType(), common.ErrUnimplementedTypedValue)
	}

	switch innerType := optType.OptionalType.GetItem().GetType().(type) {
	case *Ydb.Type_TypeId:
		switch innerType.TypeId {
		case Ydb.Type_BOOL:
			return addTypedNull[bool](formatter, args, value.Type)
		case Ydb.Type_INT8:
			return addTypedNull[int8](formatter, args, value.Type)
		case Ydb.Type_UINT8:
			return addTypedNull[uint8](formatter, args, value.Type)
		case Ydb.Type_INT16:
			return addTypedNull[int16](formatter, args, value.Type)
		case Ydb.Type_UINT16:
			return addTypedNull[uint16](formatter, args, value.Type)
		case Ydb.Type_INT32:
			return addTypedNull[int32](formatter, args, value.Type)
		case Ydb.Type_UINT32:
			return addTypedNull[uint32](formatter, args, value.Type)
		case Ydb.Type_INT64:
			return addTypedNull[int64](formatter, args, value.Type)
		case Ydb.Type_UINT64:
			return addTypedNull[uint64](formatter, args, value.Type)
		case Ydb.Type_STRING:
			return addTypedNull[[]byte](formatter, args, value.Type)
		case Ydb.Type_UTF8:
			return addTypedNull[string](formatter, args, value.Type)
		default:
			return "", args, fmt.Errorf("unsupported primitive type '%v' instead: %w", innerType, common.ErrUnimplementedTypedValue)
		}
	default:
		return "", args, fmt.Errorf("unsupported type '%v' instead: %w", innerType, common.ErrUnimplementedTypedValue)
	}
}

func formatColumn(formatter SQLFormatter, args *QueryArgs, col string) (string, *QueryArgs, error) {
	return formatter.SanitiseIdentifier(col), args, nil
}

func formatNull(_ SQLFormatter, args *QueryArgs, _ *api_service_protos.TExpression_TNull) (string, *QueryArgs, error) {
	return "NULL", args, nil
}

func formatArithmeticalExpression(
	formatter SQLFormatter,
	args *QueryArgs,
	expression *api_service_protos.TExpression_TArithmeticalExpression,
) (string, *QueryArgs, error) {
	var operation string

	switch op := expression.Operation; op {
	case api_service_protos.TExpression_TArithmeticalExpression_MUL:
		operation = " * "
	case api_service_protos.TExpression_TArithmeticalExpression_ADD:
		operation = " + "
	case api_service_protos.TExpression_TArithmeticalExpression_SUB:
		operation = " - "
	case api_service_protos.TExpression_TArithmeticalExpression_BIT_AND:
		operation = " & "
	case api_service_protos.TExpression_TArithmeticalExpression_BIT_OR:
		operation = " | "
	case api_service_protos.TExpression_TArithmeticalExpression_BIT_XOR:
		operation = " ^ "
	default:
		return "", args, fmt.Errorf("%w, op: %d", common.ErrUnimplementedArithmeticalExpression, op)
	}

	left, args, err := formatExpression(formatter, args, expression.LeftValue)
	if err != nil {
		return "", args, fmt.Errorf("failed to format left argument: %w", err)
	}

	right, args, err := formatExpression(formatter, args, expression.RightValue)
	if err != nil {
		return "", args, fmt.Errorf("failed to format right argument: %w", err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), args, nil
}

func formatExpression(formatter SQLFormatter, args *QueryArgs, expression *api_service_protos.TExpression) (string, *QueryArgs, error) {
	if !formatter.SupportsPushdownExpression(expression) {
		return "", args, common.ErrUnsupportedExpression
	}

	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return formatColumn(formatter, args, e.Column)
	case *api_service_protos.TExpression_TypedValue:
		return formatValue(formatter, args, e.TypedValue)
	case *api_service_protos.TExpression_ArithmeticalExpression:
		return formatArithmeticalExpression(formatter, args, e.ArithmeticalExpression)
	case *api_service_protos.TExpression_Null:
		return formatNull(formatter, args, e.Null)
	default:
		return "", args, fmt.Errorf("%w, type: %T", common.ErrUnimplementedExpression, e)
	}
}

func formatComparison(
	formatter SQLFormatter,
	args *QueryArgs,
	comparison *api_service_protos.TPredicate_TComparison,
) (string, *QueryArgs, error) {
	var operation string

	switch op := comparison.Operation; op {
	case api_service_protos.TPredicate_TComparison_L:
		operation = " < "
	case api_service_protos.TPredicate_TComparison_LE:
		operation = " <= "
	case api_service_protos.TPredicate_TComparison_EQ:
		operation = " = "
	case api_service_protos.TPredicate_TComparison_NE:
		operation = " <> "
	case api_service_protos.TPredicate_TComparison_GE:
		operation = " >= "
	case api_service_protos.TPredicate_TComparison_G:
		operation = " > "
	default:
		return "", args, fmt.Errorf("%w, op: %d", common.ErrUnimplementedOperation, op)
	}

	left, args, err := formatExpression(formatter, args, comparison.LeftValue)
	if err != nil {
		return "", args, fmt.Errorf("failed to format left argument: %w", err)
	}

	right, args, err := formatExpression(formatter, args, comparison.RightValue)
	if err != nil {
		return "", args, fmt.Errorf("failed to format right argument: %w", err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), args, nil
}

func formatNegation(
	formatter SQLFormatter,
	args *QueryArgs,
	negation *api_service_protos.TPredicate_TNegation) (string, *QueryArgs, error) {
	pred, args, err := formatPredicate(formatter, args, negation.Operand, false)
	if err != nil {
		return "", args, fmt.Errorf("failed to format NOT statement: %w", err)
	}

	return fmt.Sprintf("(NOT %s)", pred), args, nil
}

func formatConjunction(
	formatter SQLFormatter,
	args *QueryArgs,
	conjunction *api_service_protos.TPredicate_TConjunction,
	topLevel bool,
) (string, *QueryArgs, error) {
	var (
		sb        strings.Builder
		succeeded int32
		statement string
		err       error
		first     string
	)

	for _, predicate := range conjunction.Operands {
		argsCut := args
		statement, args, err = formatPredicate(formatter, args, predicate, false)

		if err != nil {
			if !topLevel {
				return "", args, fmt.Errorf("failed to format AND statement: %w", err)
			}

			args = argsCut
		} else {
			if succeeded > 0 {
				if succeeded == 1 {
					sb.WriteString("(")
					sb.WriteString(first)
				}

				sb.WriteString(" AND ")
				sb.WriteString(statement)
			} else {
				first = statement
			}

			succeeded++
		}
	}

	if succeeded == 0 {
		return "", args, fmt.Errorf("failed to format AND statement: %w", err)
	}

	if succeeded == 1 {
		sb.WriteString(first)
	} else {
		sb.WriteString(")")
	}

	return sb.String(), args, nil
}

func formatDisjunction(
	formatter SQLFormatter,
	args *QueryArgs,
	disjunction *api_service_protos.TPredicate_TDisjunction,
) (string, *QueryArgs, error) {
	var (
		sb        strings.Builder
		cnt       int32
		statement string
		err       error
		first     string
	)

	for _, predicate := range disjunction.Operands {
		statement, args, err = formatPredicate(formatter, args, predicate, false)
		if err != nil {
			return "", args, fmt.Errorf("failed to format OR statement: %w", err)
		}

		if cnt > 0 {
			if cnt == 1 {
				sb.WriteString("(")
				sb.WriteString(first)
			}

			sb.WriteString(" OR ")
			sb.WriteString(statement)
		} else {
			first = statement
		}

		cnt++
	}

	if cnt == 0 {
		return "", args, fmt.Errorf("failed to format OR statement: no operands")
	}

	if cnt == 1 {
		sb.WriteString(first)
	} else {
		sb.WriteString(")")
	}

	return sb.String(), args, nil
}

func formatIsNull(
	formatter SQLFormatter,
	args *QueryArgs,
	isNull *api_service_protos.TPredicate_TIsNull,
) (string, *QueryArgs, error) {
	statement, args, err := formatExpression(formatter, args, isNull.Value)
	if err != nil {
		return "", args, fmt.Errorf("failed to format IS NULL statement: %w", err)
	}

	return fmt.Sprintf("(%s IS NULL)", statement), args, nil
}

func formatIsNotNull(
	formatter SQLFormatter,
	args *QueryArgs,
	isNotNull *api_service_protos.TPredicate_TIsNotNull,
) (string, *QueryArgs, error) {
	statement, args, err := formatExpression(formatter, args, isNotNull.Value)
	if err != nil {
		return "", args, fmt.Errorf("failed to format IS NOT NULL statement: %w", err)
	}

	return fmt.Sprintf("(%s IS NOT NULL)", statement), args, nil
}

func formatPredicate(
	formatter SQLFormatter,
	args *QueryArgs,
	predicate *api_service_protos.TPredicate,
	topLevel bool,
) (string, *QueryArgs, error) {
	switch p := predicate.Payload.(type) {
	case *api_service_protos.TPredicate_Negation:
		return formatNegation(formatter, args, p.Negation)
	case *api_service_protos.TPredicate_Conjunction:
		return formatConjunction(formatter, args, p.Conjunction, topLevel)
	case *api_service_protos.TPredicate_Disjunction:
		return formatDisjunction(formatter, args, p.Disjunction)
	case *api_service_protos.TPredicate_IsNull:
		return formatIsNull(formatter, args, p.IsNull)
	case *api_service_protos.TPredicate_IsNotNull:
		return formatIsNotNull(formatter, args, p.IsNotNull)
	case *api_service_protos.TPredicate_Comparison:
		return formatComparison(formatter, args, p.Comparison)
	case *api_service_protos.TPredicate_BoolExpression:
		return formatExpression(formatter, args, p.BoolExpression.Value)
	default:
		return "", args, fmt.Errorf("%w, type: %T", common.ErrUnimplementedPredicateType, p)
	}
}

func formatWhereClause(formatter SQLFormatter, where *api_service_protos.TSelect_TWhere) (string, *QueryArgs, error) {
	if where.FilterTyped == nil {
		return "", nil, fmt.Errorf("unexpected nil filter: %w", common.ErrInvalidRequest)
	}

	args := &QueryArgs{}
	formatted, args, err := formatPredicate(formatter, args, where.FilterTyped, true)

	if err != nil {
		return "", nil, err
	}

	result := "WHERE " + formatted

	return result, args, nil
}
