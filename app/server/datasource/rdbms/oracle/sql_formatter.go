package oracle

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
}

func (sqlFormatter) supportsType(typeID Ydb.Type_PrimitiveTypeId) bool {
	switch typeID {
	// case Ydb.Type_BOOL:  // TODO: YQ-3527
	// 	return true
	// case Ydb.Type_INT8:
	// 	return true
	// case Ydb.Type_UINT8:
	// 	return true
	// case Ydb.Type_INT16:
	// 	return true
	// case Ydb.Type_UINT16:
	// 	return true
	// case Ydb.Type_INT32:
	// 	return true
	// case Ydb.Type_UINT32:
	// 	return true
	case Ydb.Type_INT64:
		return true
	// case Ydb.Type_UINT64: // TODO: YQ-3527
	// 	return true
	// case Ydb.Type_FLOAT:  // TODO: YQ-3498
	// 	return true
	case Ydb.Type_DOUBLE:
		return true
	default:
		return false
	}
}

func (f sqlFormatter) supportsConstantValueExpression(t *Ydb.Type) bool {
	// TODO: test pushdown
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
	// TODO: test pushdown
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
	return fmt.Sprintf(":%d", n+1)
}

func (sqlFormatter) SanitiseIdentifier(ident string) string {
	sanitizedIdent := strings.ReplaceAll(ident, string([]byte{0}), "")
	sanitizedIdent = `"` + strings.ReplaceAll(sanitizedIdent, `"`, `""`) + `"`

	return sanitizedIdent
}

func (f sqlFormatter) FormatFrom(params *rdbms_utils.SQLFormatterFormatFromParams) (string, error) {
	return f.SanitiseIdentifier(params.TableName), nil
}

func NewSQLFormatter() rdbms_utils.SQLFormatter {
	return sqlFormatter{}
}
