package ydb

import (

	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	api_service_protos "github.com/ydb-platform/fq-connector-go/libgo/service/protos"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
}

// for pushdouwn feature
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
	case Ydb.Type_STRING:
		return true
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
	return false // TODO: pushdown support
}

func (f sqlFormatter) GetPlaceholder(_ int) string {
	return "?"
}

//  TODO: add identifiers processing
func (f sqlFormatter) SanitiseIdentifier(ident string) string {
	return ident
}

func NewSQLFormatter() rdbms_utils.SQLFormatter {
	return sqlFormatter{}
}
