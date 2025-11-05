package utils //nolint:revive

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/decimal"
	"github.com/ydb-platform/fq-connector-go/common"
)

// SQL query predicate construction process has some context. Here is it:
type predicateBuilder struct {
	formatter SQLFormatter
	args      *QueryArgs

	// In some filtering modes it's possible to suppress errors occurred during
	// conjunction predicate construction.
	conjunctionErrors []error

	// Abstraction leaked a bit.
	// Remove this field after YQ-4191, KIKIMR-22852 is fixed.
	dataSourceKind api_common.EGenericDataSourceKind
}

func (pb *predicateBuilder) formatValue(
	value *Ydb.TypedValue,
	embedBool bool, // remove after YQ-4191, KIKIMR-22852 is fixed
) (string, error) {
	if value.Type.GetOptionalType() != nil {
		return pb.formatOptionalValue(value)
	}

	return pb.formatTypedValue(value, embedBool)
}

//nolint:gocyclo
func (pb *predicateBuilder) formatTypedValue(
	value *Ydb.TypedValue,
	embedBool bool, // remove after YQ-4191, KIKIMR-22852 is fixed
) (string, error) {
	switch v := value.Value.Value.(type) {
	case *Ydb.Value_BoolValue:
		// This is a workaround for troubles with COALESCE pushdown in Cloud Logging
		if embedBool {
			if value.Value.GetBoolValue() {
				return "true", nil
			}

			return "false", nil
		}

		pb.args.AddTyped(value.Type, v.BoolValue)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_Int32Value:
		pb.args.AddTyped(value.Type, v.Int32Value)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_Uint32Value:
		pb.args.AddTyped(value.Type, v.Uint32Value)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_Int64Value:
		switch value.Type.GetTypeId() {
		case Ydb.Type_INT64:
			pb.args.AddTyped(value.Type, v.Int64Value)

			return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
		case Ydb.Type_TIMESTAMP:
			// YQL Timestamp is always UTC
			pb.args.AddTyped(value.Type, time.UnixMicro(v.Int64Value).UTC())

			return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
		default:
			return "", fmt.Errorf("unsupported type '%T': %w", v, common.ErrUnimplementedTypedValue)
		}
	case *Ydb.Value_Uint64Value:
		pb.args.AddTyped(value.Type, v.Uint64Value)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_FloatValue:
		pb.args.AddTyped(value.Type, v.FloatValue)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_DoubleValue:
		pb.args.AddTyped(value.Type, v.DoubleValue)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_BytesValue:
		switch t := value.Type.Type.(type) {
		case *Ydb.Type_TypeId:
			pb.args.AddTyped(value.Type, v.BytesValue)

			return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
		case *Ydb.Type_DecimalType:
			decimalValue := decimal.Deserialize(v.BytesValue, t.DecimalType.Scale)
			pb.args.AddTyped(value.Type, decimalValue)

			return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
		default:
			return "", fmt.Errorf("unsupported type '%T' for bytes value: %w", v, common.ErrUnimplementedTypedValue)
		}
	case *Ydb.Value_TextValue:
		pb.args.AddTyped(value.Type, v.TextValue)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
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

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_Int32Value:
		pb.args.AddTyped(value.Type, &v.Int32Value)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_Uint32Value:
		pb.args.AddTyped(value.Type, &v.Uint32Value)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_Int64Value:
		pb.args.AddTyped(value.Type, &v.Int64Value)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_Uint64Value:
		pb.args.AddTyped(value.Type, &v.Uint64Value)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_FloatValue:
		pb.args.AddTyped(value.Type, &v.FloatValue)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_DoubleValue:
		pb.args.AddTyped(value.Type, &v.DoubleValue)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_BytesValue:
		pb.args.AddTyped(value.Type, &v.BytesValue)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
	case *Ydb.Value_TextValue:
		pb.args.AddTyped(value.Type, &v.TextValue)

		return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
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

	return pb.formatter.GetPlaceholder(pb.args.Count() - 1), nil
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

func (pb *predicateBuilder) formatColumn(col string) string {
	return pb.formatter.SanitiseIdentifier(col)
}

func (*predicateBuilder) formatNull(_ *api_service_protos.TExpression_TNull) (string, error) {
	return "NULL", nil
}

func (pb *predicateBuilder) formatArithmeticalExpression(
	expression *api_service_protos.TExpression_TArithmeticalExpression,
	embedBool bool, // remove after YQ-4191, KIKIMR-22852 is fixed
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

	left, err := pb.formatExpression(expression.LeftValue, embedBool)
	if err != nil {
		return "", fmt.Errorf("format left expression %v: %w", expression.LeftValue, err)
	}

	right, err := pb.formatExpression(expression.RightValue, embedBool)
	if err != nil {
		return "", fmt.Errorf("format right expression %v: %w", expression.RightValue, err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), nil
}

func (pb *predicateBuilder) formatExpression(
	expression *api_service_protos.TExpression,
	embedBool bool, // remove after YQ-4191, KIKIMR-22852 is fixed
) (string, error) {
	if !pb.formatter.SupportsExpression(expression) {
		return "", common.ErrUnsupportedExpression
	}

	var (
		result string
		err    error
	)

	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		result = pb.formatColumn(e.Column)
	case *api_service_protos.TExpression_TypedValue:
		result, err = pb.formatValue(e.TypedValue, embedBool)
		if err != nil {
			return result, fmt.Errorf("format value: %w", err)
		}
	case *api_service_protos.TExpression_ArithmeticalExpression:
		result, err = pb.formatArithmeticalExpression(e.ArithmeticalExpression, embedBool)
		if err != nil {
			return result, fmt.Errorf("format arithmetical expression: %w", err)
		}
	case *api_service_protos.TExpression_Null:
		result, err = pb.formatNull(e.Null)
		if err != nil {
			return result, fmt.Errorf("format null: %w", err)
		}
	case *api_service_protos.TExpression_If:
		result, err = pb.formatIf(e.If, embedBool)
		if err != nil {
			return result, fmt.Errorf("format if expression '%v': %w", e.If, err)
		}
	case *api_service_protos.TExpression_Cast:
		result, err = pb.formatCast(e.Cast, embedBool)
		if err != nil {
			return result, fmt.Errorf("format cast expression '%v': %w", e.Cast, err)
		}
	default:
		return "", fmt.Errorf("type: %T: %w", e, common.ErrUnimplementedExpression)
	}

	return result, nil
}

func (pb *predicateBuilder) formatIf(
	expression *api_service_protos.TExpression_TIf,
	embedBool bool,
) (string, error) {
	predicateExpr, err := pb.formatPredicate(expression.Predicate, false, embedBool)
	if err != nil {
		return "", fmt.Errorf("format predicate: %w", err)
	}

	thenExpr, err := pb.formatExpression(expression.ThenExpression, embedBool)
	if err != nil {
		return "", fmt.Errorf("format then expression: %w", err)
	}

	elseExpr, err := pb.formatExpression(expression.ElseExpression, embedBool)
	if err != nil {
		return "", fmt.Errorf("format else expression: %w", err)
	}

	result, err := pb.formatter.FormatIf(predicateExpr, thenExpr, elseExpr)
	if err != nil {
		return "", fmt.Errorf("formatter format if expression: %w", err)
	}

	return result, nil
}

func (pb *predicateBuilder) formatCast(
	expression *api_service_protos.TExpression_TCast,
	embedBool bool,
) (string, error) {
	// Format the value to be cast
	valueExpr, err := pb.formatExpression(expression.Value, embedBool)
	if err != nil {
		return "", fmt.Errorf("format value: %w", err)
	}

	result, err := pb.formatter.FormatCast(valueExpr, expression.Type)
	if err != nil {
		return "", fmt.Errorf("formatter format cast: %w", err)
	}

	return result, nil
}

//nolint:gocyclo
func (pb *predicateBuilder) formatComparison(
	comparisonInitial *api_service_protos.TPredicate_TComparison,
	embedBool bool, // remove after YQ-4191, KIKIMR-22852 is fixed
) (string, error) {
	// transform comparison node taking into account data source specifics
	comparison, err := pb.formatter.TransformPredicateComparison(comparisonInitial)
	if err != nil {
		return "", fmt.Errorf("transform predicate comparison: %w", err)
	}

	// render left and right operands
	left, err := pb.formatExpression(comparison.LeftValue, embedBool)
	if err != nil {
		return "", fmt.Errorf("format left expression: %v: %w", comparison.LeftValue, err)
	}

	right, err := pb.formatExpression(comparison.RightValue, embedBool)
	if err != nil {
		return "", fmt.Errorf("format right expression: %v: %w", comparison.RightValue, err)
	}

	// a special branch to handle predicates related to LIKE operator
	switch op := comparison.Operation; op {
	case api_service_protos.TPredicate_TComparison_STARTS_WITH:
		result, err := pb.formatter.FormatStartsWith(left, right)
		if err != nil {
			return "", fmt.Errorf("format starts with: %w", err)
		}

		return result, nil
	case api_service_protos.TPredicate_TComparison_ENDS_WITH:
		result, err := pb.formatter.FormatEndsWith(left, right)
		if err != nil {
			return "", fmt.Errorf("format ends with: %w", err)
		}

		return result, nil
	case api_service_protos.TPredicate_TComparison_CONTAINS:
		result, err := pb.formatter.FormatContains(left, right)
		if err != nil {
			return "", fmt.Errorf("format contains: %w", err)
		}

		return result, nil
	default:
	}

	// check basic operations
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
		return "", fmt.Errorf(
			"operation %s, op: %w",
			api_service_protos.TPredicate_TComparison_EOperation_name[int32(op)],
			common.ErrUnimplementedOperation,
		)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), nil
}

func (pb *predicateBuilder) formatNegation(
	negation *api_service_protos.TPredicate_TNegation) (string, error) {
	pred, err := pb.formatPredicate(negation.Operand, false, false)
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
		statement, err = pb.formatPredicate(predicate, false, false)
		if err != nil {
			if !topLevel {
				return "", fmt.Errorf("format predicate: %w", err)
			}

			// For some filtering modes this kind of errors may be considered as non-fatal.
			pb.conjunctionErrors = append(pb.conjunctionErrors, fmt.Errorf("format predicate: %w", err))
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
		statement, err = pb.formatPredicate(predicate, false, true)
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
		return "", errors.New("no operands")
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
	statement, err := pb.formatExpression(isNull.Value, false)
	if err != nil {
		return "", fmt.Errorf("format expression: %w", err)
	}

	return fmt.Sprintf("(%s IS NULL)", statement), nil
}

func (pb *predicateBuilder) formatIsNotNull(
	isNotNull *api_service_protos.TPredicate_TIsNotNull,
) (string, error) {
	statement, err := pb.formatExpression(isNotNull.Value, false)
	if err != nil {
		return "", fmt.Errorf("format expression: %w", err)
	}

	return fmt.Sprintf("(%s IS NOT NULL)", statement), nil
}

func (pb *predicateBuilder) formatCoalesce(
	coalesce *api_service_protos.TPredicate_TCoalesce,
) (string, error) {
	// Abstraction leaked a bit.
	// Remove this field after YQ-4191, KIKIMR-22852 is fixed.
	embedBool := pb.dataSourceKind == api_common.EGenericDataSourceKind_LOGGING

	var sb strings.Builder

	sb.WriteString("COALESCE(")

	for i, op := range coalesce.Operands {
		statement, err := pb.formatPredicate(op, false, embedBool)
		if err != nil {
			return "", fmt.Errorf("format expression: %w", err)
		}

		sb.WriteString(statement)

		if i < len(coalesce.Operands)-1 {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(")")

	return sb.String(), nil
}

func (pb *predicateBuilder) formatRegexp(
	regexp *api_service_protos.TPredicate_TRegexp,
	embedBool bool,
) (string, error) {
	valueStr, err := pb.formatExpression(regexp.Value, embedBool)
	if err != nil {
		return "", fmt.Errorf("format expression for value '%v': %w", regexp.Value, err)
	}

	patternStr, err := pb.formatExpression(regexp.Pattern, embedBool)
	if err != nil {
		return "", fmt.Errorf("format expression for pattern '%v': %w", regexp.Pattern, err)
	}

	return pb.formatter.FormatRegexp(valueStr, patternStr)
}

//nolint:gocyclo
func (pb *predicateBuilder) formatPredicate(
	predicate *api_service_protos.TPredicate,
	topLevel bool,
	embedBool bool, // remove after YQ-4191, KIKIMR-22852 is fixed
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
		result, err = pb.formatComparison(p.Comparison, embedBool)
		if err != nil {
			return "", fmt.Errorf("format comparison: %w", err)
		}
	case *api_service_protos.TPredicate_BoolExpression:
		result, err = pb.formatExpression(p.BoolExpression.Value, embedBool)
		if err != nil {
			return "", fmt.Errorf("format expression: %w", err)
		}
	case *api_service_protos.TPredicate_Coalesce:
		result, err = pb.formatCoalesce(p.Coalesce)
		if err != nil {
			return "", fmt.Errorf("format coalesce: %w", err)
		}
	case *api_service_protos.TPredicate_Regexp:
		result, err = pb.formatRegexp(p.Regexp, embedBool)
		if err != nil {
			return "", fmt.Errorf("format regexp: %w", err)
		}
	case *api_service_protos.TPredicate_Between:
		result, err = pb.FormatBetween(p.Between, embedBool)
		if err != nil {
			return "", fmt.Errorf("format between: %w", err)
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
	dataSourceKind api_common.EGenericDataSourceKind, // remove after YQ-4191, KIKIMR-22852 is fixed
) (string, *QueryArgs, error) {
	if where.FilterTyped == nil {
		return "", nil, nil
	}

	pb := &predicateBuilder{formatter: formatter, args: &QueryArgs{}, dataSourceKind: dataSourceKind}

	clause, err := pb.formatPredicate(where.FilterTyped, true, false)

	switch filtering {
	case api_service_protos.TReadSplitsRequest_FILTERING_UNSPECIFIED, api_service_protos.TReadSplitsRequest_FILTERING_OPTIONAL:
		// Pushdown error is suppressed in this mode.
		// Connector will return more data than necessary, so YDB must perform the appropriate filtering on its side.
		for _, conjunctionErr := range pb.conjunctionErrors {
			logger.Warn("failed to pushdown some parts of WHERE clause", zap.Error(conjunctionErr))
		}

		if common.OptionalFilteringAllowedErrors.Match(err) {
			logger.Warn("considering pushdown error as acceptable", zap.Error(err))

			return clause, pb.args, nil
		}

		return clause, pb.args, err
	case api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY:
		// Pushdowning every expression is mandatory in this mode.
		// If connector doesn't support some types or expressions, the request will fail.
		return clause, pb.args, err
	default:
		return "", nil, fmt.Errorf("unknown filtering mode: %d", filtering)
	}
}

func (pb *predicateBuilder) FormatBetween(
	b *api_service_protos.TPredicate_TBetween,
	embedBool bool,
) (string, error) {
	var (
		value, greatest, least string
		err                    error
	)

	value, err = pb.formatExpression(b.Value, embedBool)
	if err != nil {
		return "", fmt.Errorf("format expression for between value '%v': %w", b.Value, err)
	}

	greatest, err = pb.formatExpression(b.Greatest, embedBool)
	if err != nil {
		return "", fmt.Errorf("format expression for between greatest '%v': %w", b.Greatest, err)
	}

	least, err = pb.formatExpression(b.Least, embedBool)
	if err != nil {
		return "", fmt.Errorf("format expression for between least '%v': %w", b.Least, err)
	}

	return pb.formatter.RenderBetween(value, least, greatest)
}
