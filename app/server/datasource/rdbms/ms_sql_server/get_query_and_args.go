package ms_sql_server

import (
	_ "github.com/denisenkom/go-mssqldb"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func GetQueryAndArgs(request *api_service_protos.TDescribeTableRequest) (string, []any) {
	// opts := request.GetDataSourceInstance().GetPgOptions().GetSchema()

	query := "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'users';"
	// args := []any{request.Table} //, opts}

	return query, nil
}
