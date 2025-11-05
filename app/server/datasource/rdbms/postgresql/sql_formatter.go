package postgresql

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
	rdbms_utils.SQLFormatterDefault
	cfg *config.TPushdownConfig
}

//nolint:revive
func (f *sqlFormatter) supportsType(typeID Ydb.Type_PrimitiveTypeId) bool {
	// TODO Json_document - binary form of json
	switch typeID {
	case Ydb.Type_BOOL:
		return true
	case Ydb.Type_INT8:
		return true
	case Ydb.Type_UINT8:
		return false
	case Ydb.Type_INT16:
		return true
	case Ydb.Type_UINT16:
		return false
	case Ydb.Type_INT32:
		return true
	case Ydb.Type_UINT32:
		return false
	case Ydb.Type_INT64:
		return true
	case Ydb.Type_UINT64:
		return false
	case Ydb.Type_FLOAT:
		return true
	case Ydb.Type_DOUBLE:
		return true
	case Ydb.Type_JSON:
		return false
	case Ydb.Type_TIMESTAMP:
		return f.cfg.EnableTimestampPushdown
	default:
		return false
	}
}

func (f sqlFormatter) supportsConstantValueExpression(t *Ydb.Type) bool {
	switch v := t.Type.(type) {
	case *Ydb.Type_TypeId:
		return f.supportsType(v.TypeId)
	case *Ydb.Type_OptionalType:
		return f.supportsConstantValueExpression(v.OptionalType.Item)
	case *Ydb.Type_DecimalType:
		return true
	default:
		return false
	}
}

func (f sqlFormatter) SupportsExpression(expression *api_service_protos.TExpression) bool {
	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return true
	case *api_service_protos.TExpression_TypedValue:
		return f.supportsConstantValueExpression(e.TypedValue.Type)
	case *api_service_protos.TExpression_ArithmeticalExpression:
		return false
	case *api_service_protos.TExpression_Null:
		return true
	default:
		return false
	}
}

func (sqlFormatter) GetPlaceholder(n int) string {
	return fmt.Sprintf("$%d", n+1)
}

func (sqlFormatter) SanitiseIdentifier(ident string) string {
	// https://github.com/jackc/pgx/blob/v5.4.3/conn.go#L93
	// https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
	sanitizedIdent := strings.ReplaceAll(ident, string([]byte{0}), "")

	sanitizedIdent = `"` + strings.ReplaceAll(sanitizedIdent, `"`, `""`) + `"`

	return sanitizedIdent
}

func (f sqlFormatter) FormatWhat(what *api_service_protos.TSelect_TWhat, _ string) (string, error) {
	return rdbms_utils.FormatWhatDefault(f, what), nil
}

func (f sqlFormatter) FormatFrom(tableName string) string {
	return f.SanitiseIdentifier(tableName)
}

func (f sqlFormatter) RenderSelectQueryText(
	parts *rdbms_utils.SelectQueryParts,
	split *api_service_protos.TSplit,
) (string, error) {
	sb := &strings.Builder{}

	sb.WriteString("SELECT ")
	sb.WriteString(parts.SelectClause)
	sb.WriteString(" FROM ")
	sb.WriteString(parts.FromClause)

	var dst TSplitDescription

	// FIXME: this is the legacy behavior of Greenplum connector:
	// need to make distinct SQL formatters in PostgreSQL and Greenplum in future.
	if len(split.GetDescription()) == 0 {
		return f.renderSelectQueryTextSingle(sb, parts), nil
	}

	if err := protojson.Unmarshal(split.GetDescription(), &dst); err != nil {
		return "", fmt.Errorf("unmarshal split description: %w", err)
	}

	switch t := dst.Payload.(type) {
	case *TSplitDescription_Single:
		return f.renderSelectQueryTextSingle(sb, parts), nil
	case *TSplitDescription_HistogramBounds:
		out, err := f.renderSelectQueryTextWithHistogramBounds(sb, parts, t.HistogramBounds)
		if err != nil {
			return "", fmt.Errorf("render select query text with histogram bounds: %w", err)
		}

		return out, nil
	default:
		return "", fmt.Errorf("unknown splitting mode: %v", t)
	}
}

func (sqlFormatter) renderSelectQueryTextSingle(
	sb *strings.Builder,
	parts *rdbms_utils.SelectQueryParts,
) string {
	if parts.WhereClause != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(parts.WhereClause)
	}

	return sb.String()
}

func (f sqlFormatter) renderSelectQueryTextWithHistogramBounds(
	sb *strings.Builder,
	parts *rdbms_utils.SelectQueryParts,
	histogramBounds *TSplitDescription_THistogramBounds,
) (string, error) {
	sb.WriteString(" WHERE ")

	if parts.WhereClause != "" {
		sb.WriteString(parts.WhereClause)
		sb.WriteString(" AND ")
	}

	var lowerVal, upperVal any

	switch t := (histogramBounds.Payload).(type) {
	case *TSplitDescription_THistogramBounds_Int32Bounds:
		if t.Int32Bounds.Lower != nil {
			lowerVal = t.Int32Bounds.Lower.Value
		}

		if t.Int32Bounds.Upper != nil {
			upperVal = t.Int32Bounds.Upper.Value
		}
	case *TSplitDescription_THistogramBounds_Int64Bounds:
		if t.Int64Bounds.Lower != nil {
			lowerVal = t.Int64Bounds.Lower.Value
		}

		if t.Int64Bounds.Upper != nil {
			upperVal = t.Int64Bounds.Upper.Value
		}
	case *TSplitDescription_THistogramBounds_DecimalBounds:
		if t.DecimalBounds.Lower != nil {
			lowerVal = t.DecimalBounds.Lower.Value
		}

		if t.DecimalBounds.Upper != nil {
			upperVal = t.DecimalBounds.Upper.Value
		}
	default:
		return "", fmt.Errorf("unknown histogram bounds type: %v", t)
	}

	return f.renderSelectQueryTextWithBoundsHelper(sb, histogramBounds.ColumnName, lowerVal, upperVal)
}

func (f sqlFormatter) renderSelectQueryTextWithBoundsHelper(
	sb *strings.Builder,
	columnName string,
	lower, upper any,
) (string, error) {
	if columnName == "" {
		return "", errors.New("column name is empty")
	}

	columnName = f.SanitiseIdentifier(columnName)

	if lower == nil && upper == nil {
		return "", errors.New("you must fill either lower bounds, either upper bounds, or both of them")
	}

	if lower == nil && upper != nil {
		if _, err := fmt.Fprintf(sb, "%s < %v", columnName, upper); err != nil {
			return "", fmt.Errorf("fprintf: %w", err)
		}

		return sb.String(), nil
	}

	if lower != nil && upper == nil {
		if _, err := fmt.Fprintf(sb, "%s >= %v", columnName, lower); err != nil {
			return "", fmt.Errorf("fprintf: %w", err)
		}

		return sb.String(), nil
	}

	sb.WriteString("(")

	if _, err := fmt.Fprintf(sb,
		"%s >= %v AND %s < %v",
		columnName, lower,
		columnName, upper,
	); err != nil {
		return "", fmt.Errorf("fprintf: %w", err)
	}

	sb.WriteString(")")

	return sb.String(), nil
}

func (sqlFormatter) RenderBetween(value, least, greatest string) (string, error) {
	return fmt.Sprintf("%s BETWEEN %s AND %s", value, least, greatest), nil
}

func NewSQLFormatter(cfg *config.TPushdownConfig) rdbms_utils.SQLFormatter {
	return sqlFormatter{cfg: cfg}
}
