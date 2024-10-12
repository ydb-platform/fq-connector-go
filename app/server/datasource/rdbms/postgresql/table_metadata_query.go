package postgresql

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

func TableMetadataQuery(
	request *api_service_protos.TDescribeTableRequest,
	schema string,
) (string, *rdbms_utils.QueryArgs) {
	query := "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 AND table_schema = $2"

	var args rdbms_utils.QueryArgs

	args.AddUntyped(request.Table)
	args.AddUntyped(schema)

	return query, &args
}
