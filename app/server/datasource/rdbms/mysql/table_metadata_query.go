package mysql

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

func TableMetadataQuery(request *api_service_protos.TDescribeTableRequest) (string, *rdbms_utils.QueryArgs) {
	// TODO: do not add 'unsigned' modifiers to column type and use the driver-provided fields instead.
	// In MySQL schema and database are basically the same thing. So we can safely pass dbname as
	// `schema_name` when quering `information_schema`.
	query := `SELECT column_name, column_type FROM information_schema.columns
		WHERE table_name = ? AND table_schema = ?`

	var args rdbms_utils.QueryArgs
	args.AddUntyped(request.Table)
	args.AddUntyped(request.GetDataSourceInstance().Database)

	return query, &args
}
