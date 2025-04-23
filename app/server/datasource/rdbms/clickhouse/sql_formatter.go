package clickhouse

import (
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
	cfg *config.TPushdownConfig
}

func (f *sqlFormatter) supportsType(typeID Ydb.Type_PrimitiveTypeId) bool {
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
	case Ydb.Type_JSON:
		return false
	case Ydb.Type_TIMESTAMP:
		return f.cfg.EnableTimestampPushdown
	default:
		return false
	}
}

func (f *sqlFormatter) supportsConstantValueExpression(t *Ydb.Type) bool {
	switch v := t.Type.(type) {
	case *Ydb.Type_TypeId:
		return f.supportsType(v.TypeId)
	case *Ydb.Type_OptionalType:
		return f.supportsConstantValueExpression(v.OptionalType.Item)
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

func (sqlFormatter) GetPlaceholder(_ int) string {
	return "?"
}

func (sqlFormatter) SanitiseIdentifier(ident string) string {
	// https: //clickhouse.com/docs/en/sql-reference/syntax#identifiers
	sanitizedIdent := strings.ReplaceAll(ident, string([]byte{0}), "")
	sanitizedIdent = `"` + strings.ReplaceAll(sanitizedIdent, `"`, `""`) + `"`

	return sanitizedIdent
}

func (f sqlFormatter) FormatFrom(tableName string) string {
	return f.SanitiseIdentifier(tableName)
}

func (sqlFormatter) RenderSelectQueryText(
	parts *rdbms_utils.SelectQueryParts,
	_ *api_service_protos.TSplit,
) (string, error) {
	return rdbms_utils.DefaultSelectQueryRender(parts)
}

func NewSQLFormatter(cfg *config.TPushdownConfig) rdbms_utils.SQLFormatter {
	return sqlFormatter{cfg: cfg}
}
