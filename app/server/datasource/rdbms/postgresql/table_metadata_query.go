package postgresql

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func TableMetadataQuery(
	request *api_service_protos.TDescribeTableRequest,
	schema string,
) (string, []any) {
	query := "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 AND table_schema = $2"
	args := []any{request.Table, schema}

	return query, args
}
