package oracle

import (
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

func TableMetadataQuery(
	dsi *api_common.TDataSourceInstance,
	tableName string) (string, *rdbms_utils.QueryArgs) {
	// TODO YQ-3413: synonym tables and from other users.
	// TODO YQ-3454: all capitalize
	query := "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :1"

	var args rdbms_utils.QueryArgs

	args.AddUntyped(tableName)

	return query, &args
}
