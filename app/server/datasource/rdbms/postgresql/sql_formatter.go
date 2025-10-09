package postgresql

import (
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

	switch t := (histogramBounds.Payload).(type) {
	case *TSplitDescription_THistogramBounds_Int64Bounds:
		out, err := f.renderSelectQueryTextWithInt64Bounds(sb, histogramBounds.ColumnName, t.Int64Bounds)
		if err != nil {
			return "", fmt.Errorf("render select query text with int64 bounds: %w", err)
		}

		return out, nil
	default:
		return "", fmt.Errorf("unknown histogram bounds type: %v", t)
	}
}

func (f sqlFormatter) renderSelectQueryTextWithInt64Bounds(
	sb *strings.Builder,
	columnName string,
	bounds *TInt64Bounds,
) (string, error) {
	if columnName == "" {
		return "", fmt.Errorf("column name is empty")
	}

	columnName = f.SanitiseIdentifier(columnName)

	if bounds.Lower == nil && bounds.Upper == nil {
		return "", fmt.Errorf("you must fill either lower bounds, either upper bounds, or both of them")
	}

	if bounds.Lower == nil && bounds.Upper != nil {
		if _, err := fmt.Fprintf(sb, "%s < %d", columnName, bounds.Upper.Value); err != nil {
			return "", fmt.Errorf("fprintf: %w", err)
		}

		return sb.String(), nil
	}

	if bounds.Lower != nil && bounds.Upper == nil {
		if _, err := fmt.Fprintf(sb, "%s >= %d", columnName, bounds.Lower.Value); err != nil {
			return "", fmt.Errorf("fprintf: %w", err)
		}

		return sb.String(), nil
	}

	sb.WriteString("(")

	if _, err := fmt.Fprintf(sb,
		"%s >= %d AND %s < %d",
		columnName, bounds.Lower.Value,
		columnName, bounds.Upper.Value,
	); err != nil {
		return "", fmt.Errorf("fprintf: %w", err)
	}

	sb.WriteString(")")

	return sb.String(), nil
}

func NewSQLFormatter(cfg *config.TPushdownConfig) rdbms_utils.SQLFormatter {
	return sqlFormatter{cfg: cfg}
}
