package ydb

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/encoding/protojson"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
	mode config.TYdbConfig_Mode
	cfg  *config.TPushdownConfig
}

//nolint:gocyclo
func (f sqlFormatter) supportsTypeForPushdown(typeID Ydb.Type_PrimitiveTypeId) bool {
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

func (f sqlFormatter) supportsConstantValueExpression(t *Ydb.Type) bool {
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

func (f sqlFormatter) SupportsPushdownExpression(expression *api_service_protos.TExpression) bool {
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

func (f sqlFormatter) GetPlaceholder(id int) string {
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
func (sqlFormatter) SanitiseIdentifier(ident string) string {
	return fmt.Sprintf("`%s`", ident)
}

func (f sqlFormatter) FormatFrom(tableName string) string {
	// Trim leading slash, otherwise TablePathPrefix won't work.
	// See https://ydb.tech/docs/ru/yql/reference/syntax/pragma#table-path-prefix
	tableName = strings.TrimPrefix(tableName, "/")

	return f.SanitiseIdentifier(tableName)
}

func (f sqlFormatter) RenderSelectQueryText(
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

	switch splitDescription.GetPayload().(type) {
	case *TSplitDescription_ColumnShard:
		queryText, err = f.renderSelectQueryTextForColumnShard(parts, split, splitDescription.GetColumnShard())
		if err != nil {
			return "", fmt.Errorf("render select query text for column shard: %w", err)
		}
	case *TSplitDescription_DataShard:
		queryText, err = f.renderSelectQueryTextForDataShard(parts, split, splitDescription.GetDataShard())
		if err != nil {
			return "", fmt.Errorf("render select query text for column shard: %w", err)
		}
	}

	return queryText, nil
}

func (sqlFormatter) renderSelectQueryTextForColumnShard(
	parts *rdbms_utils.SelectQueryParts,
	split *api_service_protos.TSplit,
	columnShardDescription *TSplitDescription_TColumnShard,
) (string, error) {
	// WITH TabletId='72075186224054918'
	head, err := rdbms_utils.DefaultSelectQueryRender(parts, split)
	if err != nil {
		return "", fmt.Errorf("default select query render: %w", err)
	}

	if len(columnShardDescription.TabletIds) != 1 {
		return "", fmt.Errorf(
			"column shard split description must contain exactly 1 shard id, have %d instead",
			len(columnShardDescription.TabletIds),
		)
	}

	result := head + fmt.Sprintf(" WITH TabletId='%d')", columnShardDescription.TabletIds[0])

	return result, nil
}

func (sqlFormatter) renderSelectQueryTextForDataShard(
	parts *rdbms_utils.SelectQueryParts,
	split *api_service_protos.TSplit,
	_ *TSplitDescription_TDataShard,
) (string, error) {
	queryText, err := rdbms_utils.DefaultSelectQueryRender(parts, split)
	if err != nil {
		return "", fmt.Errorf("default select query render: %w", err)
	}

	return queryText, nil
}

func NewSQLFormatter(mode config.TYdbConfig_Mode, cfg *config.TPushdownConfig) rdbms_utils.SQLFormatter {
	return sqlFormatter{
		mode: mode,
		cfg:  cfg,
	}
}
