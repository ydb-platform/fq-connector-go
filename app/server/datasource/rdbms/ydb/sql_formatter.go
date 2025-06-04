package ydb

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.SQLFormatter = (*SQLFormatter)(nil)

type SQLFormatter struct {
	rdbms_utils.SQLFormatterDefault
	mode config.TYdbConfig_Mode
	cfg  *config.TPushdownConfig
}

//nolint:gocyclo
func (f SQLFormatter) supportsTypeForPushdown(typeID Ydb.Type_PrimitiveTypeId) bool {
	switch typeID {
	case Ydb.Type_BOOL:
		return true
	case Ydb.Type_INT8:
		return true
	case Ydb.Type_UINT8:
		return true
	case Ydb.Type_INT16:
		return true
	case Ydb.Type_UINT16:
		return true
	case Ydb.Type_INT32:
		return true
	case Ydb.Type_UINT32:
		return true
	case Ydb.Type_INT64:
		return true
	case Ydb.Type_UINT64:
		return true
	case Ydb.Type_FLOAT:
		return true
	case Ydb.Type_DOUBLE:
		return true
	case Ydb.Type_STRING:
		return true
	case Ydb.Type_UTF8:
		return true
	case Ydb.Type_JSON:
		return false
	case Ydb.Type_TIMESTAMP:
		return f.cfg.EnableTimestampPushdown
	default:
		return false
	}
}

func (f SQLFormatter) supportsConstantValueExpression(t *Ydb.Type) bool {
	switch v := t.Type.(type) {
	case *Ydb.Type_TypeId:
		return f.supportsTypeForPushdown(v.TypeId)
	case *Ydb.Type_OptionalType:
		return f.supportsConstantValueExpression(v.OptionalType.Item)
	case *Ydb.Type_NullType:
		return true
	default:
		return false
	}
}

func (f SQLFormatter) SupportsExpression(expression *api_service_protos.TExpression) bool {
	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return true
	case *api_service_protos.TExpression_TypedValue:
		return f.supportsConstantValueExpression(e.TypedValue.Type)
	case *api_service_protos.TExpression_ArithmeticalExpression:
		return false
	case *api_service_protos.TExpression_Null:
		return true
	case *api_service_protos.TExpression_If:
		return f.SupportsExpression(e.If.ThenExpression) && f.SupportsExpression(e.If.ElseExpression)
	case *api_service_protos.TExpression_Cast:
		return f.SupportsExpression(e.Cast.Value)
	default:
		return false
	}
}

func (f SQLFormatter) GetPlaceholder(id int) string {
	switch f.mode {
	case config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE:
		return fmt.Sprintf("$p%d", id)
	case config.TYdbConfig_MODE_TABLE_SERVICE_STDLIB_SCAN_QUERIES:
		return "?"
	default:
		panic("unknown mode")
	}
}

// TODO: add identifiers processing
func (SQLFormatter) SanitiseIdentifier(ident string) string {
	return fmt.Sprintf("`%s`", ident)
}

func (f SQLFormatter) FormatFrom(tableName string) string {
	// Trim leading slash, otherwise TablePathPrefix won't work.
	// See https://ydb.tech/docs/ru/yql/reference/syntax/pragma#table-path-prefix
	tableName = strings.TrimPrefix(tableName, "/")

	return f.SanitiseIdentifier(tableName)
}

func (f SQLFormatter) RenderSelectQueryText(
	parts *rdbms_utils.SelectQueryParts,
	split *api_service_protos.TSplit,
) (string, error) {
	// Deserialize split description
	var (
		splitDescription TSplitDescription
		err              error
	)

	if err = protojson.Unmarshal(split.GetDescription(), &splitDescription); err != nil {
		return "", fmt.Errorf("unmarshal split description: %w", err)
	}

	var queryText string

	switch t := splitDescription.GetPayload().(type) {
	case *TSplitDescription_ColumnShard:
		queryText, err = f.RenderSelectQueryTextForColumnShard(parts, splitDescription.GetColumnShard().TabletIds)
		if err != nil {
			return "", fmt.Errorf("render select query text for column shard: %w", err)
		}
	case *TSplitDescription_DataShard:
		queryText, err = f.renderSelectQueryTextForDataShard(parts, splitDescription.GetDataShard())
		if err != nil {
			return "", fmt.Errorf("render select query text for column shard: %w", err)
		}
	default:
		return "", fmt.Errorf("unknown split description type: %T (%v)", t, t)
	}

	return queryText, nil
}

func (SQLFormatter) RenderSelectQueryTextForColumnShard(
	parts *rdbms_utils.SelectQueryParts,
	tabletIDs []uint64,
) (string, error) {
	var sb strings.Builder

	sb.WriteString("SELECT ")
	sb.WriteString(parts.SelectClause)
	sb.WriteString(" FROM ")
	sb.WriteString(parts.FromClause)

	switch len(tabletIDs) {
	case 0:
		// It's possible when reading empty OLAP tables
	case 1:
		sb.WriteString(fmt.Sprintf(" WITH TabletId='%d'", tabletIDs[0]))
	default:
		return "", fmt.Errorf("column shard split description must contain either 0, or 1 tablet id")
	}

	if parts.WhereClause != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(parts.WhereClause)
	}

	return sb.String(), nil
}

func (f SQLFormatter) renderSelectQueryTextForDataShard(
	parts *rdbms_utils.SelectQueryParts,
	_ *TSplitDescription_TDataShard,
) (string, error) {
	queryText, err := f.SQLFormatterDefault.RenderSelectQueryText(parts, nil)
	if err != nil {
		return "", fmt.Errorf("default select query render: %w", err)
	}

	return queryText, nil
}

func (SQLFormatter) FormatStartsWith(left, right string) (string, error) {
	return fmt.Sprintf("(StartsWith(%s, %s))", left, right), nil
}

func (SQLFormatter) FormatEndsWith(left, right string) (string, error) {
	return fmt.Sprintf("(EndsWith(%s, %s))", left, right), nil
}

func (SQLFormatter) FormatContains(left, right string) (string, error) {
	return fmt.Sprintf("(String::Contains(%s, %s))", left, right), nil
}

func (f SQLFormatter) FormatWhat(what *api_service_protos.TSelect_TWhat, _ string) (string, error) {
	return rdbms_utils.FormatWhatDefault(f, what), nil
}

func (SQLFormatter) FormatRegexp(left, right string) (string, error) {
	return fmt.Sprintf("(%s REGEXP %s)", left, right), nil
}

func (SQLFormatter) FormatIf(predicateExpr, thenExpr, elseExpr string) (string, error) {
	return fmt.Sprintf("IF(%s, %s, %s)", predicateExpr, thenExpr, elseExpr), nil
}

func (SQLFormatter) FormatCast(value string, ydbType *Ydb.Type) (string, error) {
	primitiveType := ydbType.GetTypeId()

	if primitiveType == Ydb.Type_PRIMITIVE_TYPE_ID_UNSPECIFIED {
		return "", fmt.Errorf("primitive type is unspecified")
	}

	typeName, err := primitiveYqlTypeName(primitiveType)
	if err != nil {
		return "", fmt.Errorf("primitive YQL type name: %w", err)
	}

	return fmt.Sprintf("CAST(%s AS %s)", value, typeName), nil
}

func NewSQLFormatter(mode config.TYdbConfig_Mode, cfg *config.TPushdownConfig) SQLFormatter {
	return SQLFormatter{
		mode: mode,
		cfg:  cfg,
	}
}
