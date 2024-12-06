package clickhouse

import (
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

func TableMetadataQuery(
	dsi *api_common.TDataSourceInstance,
	tableName string,
) (string, *rdbms_utils.QueryArgs) {
	query := "SELECT name, type FROM system.columns WHERE table = ? and database = ?"

	var args rdbms_utils.QueryArgs

	args.AddUntyped(tableName)
	args.AddUntyped(dsi.Database)

	return query, &args
}
