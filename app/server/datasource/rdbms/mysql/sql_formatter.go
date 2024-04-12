package mysql

import (
	"fmt"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
}

func (sqlFormatter) SupportsPushdownExpression(_ *api_service_protos.TExpression) bool {
	return false
}

func (sqlFormatter) GetPlaceholder(n int) string {
	return fmt.Sprintf("$%d", n+1)
}

func (sqlFormatter) SanitiseIdentifier(ident string) string {
	return ident
}

func NewSQLFormatter() rdbms_utils.SQLFormatter {
	return sqlFormatter{}
}
