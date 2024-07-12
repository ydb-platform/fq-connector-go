package oracle

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func TableMetadataQuery(request *api_service_protos.TDescribeTableRequest) (string, []any) {
	// opts := request.GetDataSourceInstance().GetPgOptions().GetSchema()
	// TODO: maybe uppercase table_name
	// TODO: check synonyms tables and tables from other schemes
	query := "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :1"
	args := []any{request.Table} // , opts}

	return query, args
}
