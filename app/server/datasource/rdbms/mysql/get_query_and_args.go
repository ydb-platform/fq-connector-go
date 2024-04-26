package mysql

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func GetQueryAndArgs(request *api_service_protos.TDescribeTableRequest) (string, []any) {
	// opts := request.GetDataSourceInstance().GetMysqlOptions().GetSchema()
	// TODO: pass table_schema using mysql options. For now fallback to db name, which is default
	query := "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? AND table_schema = ?"
	args := []any{request.Table, request.GetDataSourceInstance().Database}

	return query, args
}
