package ms_sql_server

import (
	_ "github.com/denisenkom/go-mssqldb"
	api_common "github.com/ydb-platform/fq-connector-go/api/common"

	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

func TableMetadataQuery(
	_ *api_common.TDataSourceInstance,
	tableName string,
) (string, *rdbms_utils.QueryArgs) {
	// opts := request.GetDataSourceInstance().GetPgOptions().GetSchema()
	query := "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @p1;"

	var args rdbms_utils.QueryArgs

	args.AddUntyped(tableName)

	return query, &args
}
