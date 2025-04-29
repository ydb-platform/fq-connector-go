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
	default:
		return false
	}
}

func (f SQLFormatter) SupportsPredicateComparison(
	comparison *api_service_protos.TPredicate_TComparison,
) bool {
	switch comparison.Operation {
	case
		api_service_protos.TPredicate_TComparison_L,
		api_service_protos.TPredicate_TComparison_LE,
		api_service_protos.TPredicate_TComparison_EQ,
		api_service_protos.TPredicate_TComparison_NE,
		api_service_protos.TPredicate_TComparison_GE,
		api_service_protos.TPredicate_TComparison_G,
		api_service_protos.TPredicate_TComparison_STARTS_WITH,
		api_service_protos.TPredicate_TComparison_ENDS_WITH:
		return true
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

	if len(tabletIDs) != 1 {
		return "", fmt.Errorf(
			"column shard split description must contain exactly 1 shard id, have %d instead",
			len(tabletIDs),
		)
	}

	sb.WriteString(fmt.Sprintf(" WITH TabletId='%d'", tabletIDs[0]))

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

func NewSQLFormatter(mode config.TYdbConfig_Mode, cfg *config.TPushdownConfig) SQLFormatter {
	return SQLFormatter{
		mode: mode,
		cfg:  cfg,
	}
}
