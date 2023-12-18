package clickhouse

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/libgo/service/protos"
)

func GetQueryAndArgs(request *api_service_protos.TDescribeTableRequest) (string, []any) {
	query := "SELECT name, type FROM system.columns WHERE table = ? and database = ?"
	args := []any{request.Table, request.DataSourceInstance.Database}

	return query, args
}
