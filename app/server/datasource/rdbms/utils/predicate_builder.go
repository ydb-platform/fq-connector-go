package utils

import (
	"fmt"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

// SQL query predicate construction process has some context. Here is it:
type predicateBuilder struct {
	formatter SQLFormatter
	args      *QueryArgs
	errors    []error // the errors that may occure during predicate construction
}

func (pb *predicateBuilder) formatValue(value *Ydb.TypedValue) (string, error) {
	if value.Type.GetOptionalType() != nil {
		return pb.formatOptionalValue(value)
	}

	return pb.formatPrimitiveValue(value)
}

func (pb *predicateBuilder) formatPrimitiveValue(value *Ydb.TypedValue) (string, error) {
	switch v := value.Value.Value.(type) {
	case *Ydb.Value_BoolValue:
		pb.args.AddTyped(value.Type, v.BoolValue)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_Int32Value:
		pb.args.AddTyped(value.Type, v.Int32Value)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_Uint32Value:
		pb.args.AddTyped(value.Type, v.Uint32Value)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_Int64Value:
		switch value.Type.GetTypeId() {
		case Ydb.Type_INT64:
			pb.args.AddTyped(value.Type, v.Int64Value)
			return pb.formatter.GetPlaceholder(pb.args.Count()), nil
		case Ydb.Type_TIMESTAMP:
			pb.args.AddTyped(value.Type, time.UnixMicro(v.Int64Value))
			return pb.formatter.GetPlaceholder(pb.args.Count()), nil
		default:
			return "", fmt.Errorf("unsupported type '%T': %w", v, common.ErrUnimplementedTypedValue)
		}
	case *Ydb.Value_Uint64Value:
		pb.args.AddTyped(value.Type, v.Uint64Value)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_FloatValue:
		pb.args.AddTyped(value.Type, v.FloatValue)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_DoubleValue:
		pb.args.AddTyped(value.Type, v.DoubleValue)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_BytesValue:
		pb.args.AddTyped(value.Type, v.BytesValue)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_TextValue:
		pb.args.AddTyped(value.Type, v.TextValue)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_NullFlagValue:
		placeholder, err := pb.formatNullFlagValue(value)
		if err != nil {
			return "", fmt.Errorf("format null flag value: %w", err)
		}

		return placeholder, nil
	default:
		return "", fmt.Errorf("unsupported type '%T': %w", v, common.ErrUnimplementedTypedValue)
	}
}

func (pb *predicateBuilder) formatOptionalValue(value *Ydb.TypedValue) (string, error) {
	switch v := value.Value.Value.(type) {
	case *Ydb.Value_BoolValue:
		pb.args.AddTyped(value.Type, &v.BoolValue)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_Int32Value:
		pb.args.AddTyped(value.Type, &v.Int32Value)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_Uint32Value:
		pb.args.AddTyped(value.Type, &v.Uint32Value)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_Int64Value:
		pb.args.AddTyped(value.Type, &v.Int64Value)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_Uint64Value:
		pb.args.AddTyped(value.Type, &v.Uint64Value)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_FloatValue:
		pb.args.AddTyped(value.Type, &v.FloatValue)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_DoubleValue:
		pb.args.AddTyped(value.Type, &v.DoubleValue)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_BytesValue:
		pb.args.AddTyped(value.Type, &v.BytesValue)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_TextValue:
		pb.args.AddTyped(value.Type, &v.TextValue)
		return pb.formatter.GetPlaceholder(pb.args.Count()), nil
	case *Ydb.Value_NullFlagValue:
		placeholder, err := pb.formatNullFlagValue(value)
		if err != nil {
			return "", fmt.Errorf("format null flag value: %w", err)
		}
		return placeholder, nil
	default:
		return "", fmt.Errorf("unsupported type '%T': %w", v, common.ErrUnimplementedTypedValue)
	}
}

func addTypedNull[ACCEPTOR_TYPE any](
	pb *predicateBuilder,
	ydbType *Ydb.Type,
) (string, error) {
	pb.args.AddTyped(ydbType, (*ACCEPTOR_TYPE)(nil))
	return pb.formatter.GetPlaceholder(pb.args.Count()), nil
}

func (pb *predicateBuilder) formatNullFlagValue(value *Ydb.TypedValue) (string, error) {
	optType, ok := value.Type.GetType().(*Ydb.Type_OptionalType)
	if !ok {
		return "", fmt.Errorf(
			"null flag values must be optionally typed, got type '%T' instead: %w",
			value.Type.GetType(), common.ErrUnimplementedTypedValue)
	}

	switch innerType := optType.OptionalType.GetItem().GetType().(type) {
	case *Ydb.Type_TypeId:
		switch innerType.TypeId {
		case Ydb.Type_BOOL:
			return addTypedNull[bool](pb, value.Type)
		case Ydb.Type_INT8:
			return addTypedNull[int8](pb, value.Type)
		case Ydb.Type_UINT8:
			return addTypedNull[uint8](pb, value.Type)
		case Ydb.Type_INT16:
			return addTypedNull[int16](pb, value.Type)
		case Ydb.Type_UINT16:
			return addTypedNull[uint16](pb, value.Type)
		case Ydb.Type_INT32:
			return addTypedNull[int32](pb, value.Type)
		case Ydb.Type_UINT32:
			return addTypedNull[uint32](pb, value.Type)
		case Ydb.Type_INT64:
			return addTypedNull[int64](pb, value.Type)
		case Ydb.Type_UINT64:
			return addTypedNull[uint64](pb, value.Type)
		case Ydb.Type_STRING:
			return addTypedNull[[]byte](pb, value.Type)
		case Ydb.Type_UTF8:
			return addTypedNull[string](pb, value.Type)
		default:
			return "", fmt.Errorf("unsupported primitive type '%v': %w", innerType, common.ErrUnimplementedTypedValue)
		}
	default:
		return "", fmt.Errorf("unsupported type '%v': %w", innerType, common.ErrUnimplementedTypedValue)
	}
}

func (pb *predicateBuilder) formatColumn(col string) (string, error) {
	return pb.formatter.SanitiseIdentifier(col), nil
}

func (pb *predicateBuilder) formatNull(_ *api_service_protos.TExpression_TNull) (string, error) {
	return "NULL", nil
}

func (pb *predicateBuilder) formatArithmeticalExpression(
	expression *api_service_protos.TExpression_TArithmeticalExpression,
) (string, error) {
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
		return "", fmt.Errorf("operation %d: %w", op, common.ErrUnimplementedArithmeticalExpression)
	}

	left, err := pb.formatExpression(expression.LeftValue)
	if err != nil {
		return "", fmt.Errorf("format left expression %v: %w", expression.LeftValue, err)
	}

	right, err := pb.formatExpression(expression.RightValue)
	if err != nil {
		return "", fmt.Errorf("format right expression %v: %w", expression.RightValue, err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), nil
}

func (pb *predicateBuilder) formatExpression(expression *api_service_protos.TExpression) (string, error) {
	if !pb.formatter.SupportsPushdownExpression(expression) {
		return "", common.ErrUnsupportedExpression
	}

	var (
		result string
		err    error
	)

	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		result, err = pb.formatColumn(e.Column)
		if err != nil {
			return result, fmt.Errorf("format column: %w", err)
		}
	case *api_service_protos.TExpression_TypedValue:
		result, err = pb.formatValue(e.TypedValue)
		if err != nil {
			return result, fmt.Errorf("format value: %w", err)
		}
	case *api_service_protos.TExpression_ArithmeticalExpression:
		result, err = pb.formatArithmeticalExpression(e.ArithmeticalExpression)
		if err != nil {
			return result, fmt.Errorf("format arithmetical expression: %w", err)
		}
	case *api_service_protos.TExpression_Null:
		result, err = pb.formatNull(e.Null)
		if err != nil {
			return result, fmt.Errorf("format null: %w", err)
		}
	default:
		return "", fmt.Errorf("%w, type: %T", common.ErrUnimplementedExpression, e)
	}

	return result, nil
}

func (pb *predicateBuilder) formatComparison(
	comparison *api_service_protos.TPredicate_TComparison,
) (string, error) {
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
		return "", fmt.Errorf("%w, op: %d", common.ErrUnimplementedOperation, op)
	}

	left, err := pb.formatExpression(comparison.LeftValue)
	if err != nil {
		return "", fmt.Errorf("format left expression: %v: %w", comparison.LeftValue, err)
	}

	right, err := pb.formatExpression(comparison.RightValue)
	if err != nil {
		return "", fmt.Errorf("format right expression: %v: %w", comparison.RightValue, err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), nil
}

func (pb *predicateBuilder) formatNegation(
	negation *api_service_protos.TPredicate_TNegation) (string, error) {

	pred, err := pb.formatPredicate(negation.Operand, false)
	if err != nil {
		return "", fmt.Errorf("format predicate: %w", err)
	}

	return fmt.Sprintf("(NOT %s)", pred), nil
}

func (pb *predicateBuilder) formatConjunction(
	conjunction *api_service_protos.TPredicate_TConjunction,
	topLevel bool,
) (string, error) {
	var (
		sb        strings.Builder
		succeeded int32
		statement string
		err       error
		first     string
	)

	for _, predicate := range conjunction.Operands {
		statement, err = pb.formatPredicate(predicate, false)
		fmt.Println("CRAB", statement, err)

		if err != nil {
			if !topLevel {
				return "", fmt.Errorf("format predicate: %w", err)
			}

			// For some pushdown modes, this kind of error may be considered as non-fatal.
			pb.errors = append(pb.errors, fmt.Errorf("format predicate: %w", err))
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
		return "", fmt.Errorf("format predicate: %w", err)
	}

	if succeeded == 1 {
		sb.WriteString(first)
	} else {
		sb.WriteString(")")
	}

	return sb.String(), nil
}

func (pb *predicateBuilder) formatDisjunction(
	disjunction *api_service_protos.TPredicate_TDisjunction,
) (string, error) {
	var (
		sb        strings.Builder
		cnt       int32
		statement string
		err       error
		first     string
	)

	for _, predicate := range disjunction.Operands {
		statement, err = pb.formatPredicate(predicate, false)
		if err != nil {
			return "", fmt.Errorf("format predicate: %w", err)
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
		return "", fmt.Errorf("no operands")
	}

	if cnt == 1 {
		sb.WriteString(first)
	} else {
		sb.WriteString(")")
	}

	return sb.String(), nil
}

func (pb *predicateBuilder) formatIsNull(
	isNull *api_service_protos.TPredicate_TIsNull,
) (string, error) {
	statement, err := pb.formatExpression(isNull.Value)
	if err != nil {
		return "", fmt.Errorf("format expression: %w", err)
	}

	return fmt.Sprintf("(%s IS NULL)", statement), nil
}

func (pb *predicateBuilder) formatIsNotNull(
	isNotNull *api_service_protos.TPredicate_TIsNotNull,
) (string, error) {
	statement, err := pb.formatExpression(isNotNull.Value)
	if err != nil {
		return "", fmt.Errorf("format expression: %w", err)
	}

	return fmt.Sprintf("(%s IS NOT NULL)", statement), nil
}

func (pb *predicateBuilder) formatPredicate(
	predicate *api_service_protos.TPredicate,
	topLevel bool,
) (string, error) {
	var (
		result string
		err    error
	)

	switch p := predicate.Payload.(type) {
	case *api_service_protos.TPredicate_Negation:
		result, err = pb.formatNegation(p.Negation)
		if err != nil {
			return "", fmt.Errorf("format negation: %w", err)
		}
	case *api_service_protos.TPredicate_Conjunction:
		result, err = pb.formatConjunction(p.Conjunction, topLevel)
		if err != nil {
			return "", fmt.Errorf("format conjunction: %w", err)
		}
	case *api_service_protos.TPredicate_Disjunction:
		result, err = pb.formatDisjunction(p.Disjunction)
		if err != nil {
			return "", fmt.Errorf("format disjunction: %w", err)
		}
	case *api_service_protos.TPredicate_IsNull:
		result, err = pb.formatIsNull(p.IsNull)
		if err != nil {
			return "", fmt.Errorf("format is null: %w", err)
		}
	case *api_service_protos.TPredicate_IsNotNull:
		result, err = pb.formatIsNotNull(p.IsNotNull)
		if err != nil {
			return "", fmt.Errorf("format is not null: %w", err)
		}
	case *api_service_protos.TPredicate_Comparison:
		result, err = pb.formatComparison(p.Comparison)
		if err != nil {
			return "", fmt.Errorf("format comparison: %w", err)
		}
	case *api_service_protos.TPredicate_BoolExpression:
		result, err = pb.formatExpression(p.BoolExpression.Value)
		if err != nil {
			return "", fmt.Errorf("format expression: %w", err)
		}
	default:
		return "", fmt.Errorf("%w, type: %T", common.ErrUnimplementedPredicateType, p)
	}

	return result, nil
}

func formatWhereClause(
	logger *zap.Logger,
	filtering api_service_protos.TReadSplitsRequest_EFiltering,
	formatter SQLFormatter,
	where *api_service_protos.TSelect_TWhere,
) (string, *QueryArgs, error) {
	if where.FilterTyped == nil {
		return "", nil, fmt.Errorf("unexpected nil filter: %w", common.ErrInvalidRequest)
	}

	pb := &predicateBuilder{formatter: formatter, args: &QueryArgs{}}

	formatted, err := pb.formatPredicate(where.FilterTyped, true)
	if err != nil {
		return "", nil, fmt.Errorf("format predicate: %w", err)
	}

	result := "WHERE " + formatted

	switch filtering {
	case api_service_protos.TReadSplitsRequest_FILTERING_UNSPECIFIED, api_service_protos.TReadSplitsRequest_FILTERING_OPTIONAL:
		// Pushdown error is suppressed in this mode. Connector will ask for table full scan,
		// and it's YDB is in charge for appropriate filtering
		for _, nonFatalErr := range pb.errors {
			logger.Warn("Failed to format some part of WHERE clause", zap.Error(nonFatalErr))
		}
	case api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY:
		// Pushdown is mandatory in this mode.
		// If connector doesn't support some types or expressions, the request will fail.
		break
	default:
		return "", nil, fmt.Errorf("unknown filtering mode: %d", filtering)
	}

	return result, pb.args, err
}
