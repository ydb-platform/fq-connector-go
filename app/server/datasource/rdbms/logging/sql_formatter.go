package logging

import (
	"fmt"

	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
	rdbms_utils.SQLFormatter
	resolver Resolver
}

func (f sqlFormatter) FormatFrom(params *rdbms_utils.SQLFormatterFormatFromParams) (string, error) {
	request := &resolveParams{
		ctx:          params.Ctx,
		logger:       params.Logger,
		folderId:     params.DataSourceInstance.GetLoggingOptions().FolderId,
		logGroupName: params.TableName,
	}

	response, err := f.resolver.resolve(request)
	if err != nil {
		return "", fmt.Errorf("resolve YDB table: %w", err)
	}

	return f.SanitiseIdentifier(response.tableName), nil
}

func NewSQLFormatter(resolver Resolver, mode config.TYdbConfig_Mode) rdbms_utils.SQLFormatter {
	ydbFormatter := ydb.NewSQLFormatter(mode)

	formatter := &sqlFormatter{
		SQLFormatter: ydbFormatter,
		resolver:     resolver,
	}

	return formatter
}
