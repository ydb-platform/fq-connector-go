package clickhouse

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

func TableMetadataQuery(request *api_service_protos.TDescribeTableRequest) (string, *rdbms_utils.QueryArgs) {
	query := `SELECT name, type, numeric_precision, toInt64(numeric_scale) 
			FROM system.columns WHERE table = ? and database = ?`

	var args rdbms_utils.QueryArgs

	args.AddUntyped(request.Table)
	args.AddUntyped(request.DataSourceInstance.Database)

	return query, &args
}
