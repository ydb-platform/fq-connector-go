package oracle

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

func TableMetadataQuery(request *api_service_protos.TDescribeTableRequest) (string, *rdbms_utils.QueryArgs) {
	// TODO YQ-3413: synonym tables and from other users.
	// TODO YQ-3454: all capitalize
	query := "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :1"

	var args rdbms_utils.QueryArgs

	args.AddUntyped(request.Table)

	return query, &args
}
