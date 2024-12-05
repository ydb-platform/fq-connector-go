package logging

import (
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
	rdbms_utils.SQLFormatter
	resolver resolver
}

func (f *sqlFormatter) FormatFrom(tableName string) string {
	request := &resolveParams{}
	f.resolver.resolve()
}

func NewSQLFormatter(resolver resolver, mode config.TYdbConfig_Mode) rdbms_utils.SQLFormatter {
	ydbFormatter := ydb.NewSQLFormatter(mode)

	formatter := &sqlFormatter{
		SQLFormatter: ydbFormatter,
		resolver:     resolver,
	}

	return formatter
}
