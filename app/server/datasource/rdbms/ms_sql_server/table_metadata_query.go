package ms_sql_server

import (
	_ "github.com/denisenkom/go-mssqldb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

func TableMetadataQuery(request *api_service_protos.TDescribeTableRequest) (string, *rdbms_utils.QueryArgs) {
	// opts := request.GetDataSourceInstance().GetPgOptions().GetSchema()
	query := `SELECT column_name, data_type, numeric_precision, numeric_scale 
				FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = @p1;`

	var args rdbms_utils.QueryArgs

	args.AddUntyped(request.Table)

	return query, &args
}
