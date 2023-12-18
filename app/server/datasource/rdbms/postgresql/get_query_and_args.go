package postgresql

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/libgo/service/protos"
)

func GetQueryAndArgs(request *api_service_protos.TDescribeTableRequest) (string, []any) {
	opts := request.GetDataSourceInstance().GetPgOptions().GetSchema()
	query := "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 AND table_schema = $2"
	args := []any{request.Table, opts}

	return query, args
}
