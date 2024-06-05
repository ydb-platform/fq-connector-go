package postgresql

import (
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func GetQueryAndArgs(request *api_service_protos.TDescribeTableRequest) (string, []any) {
	var opts string

	if request.DataSourceInstance.Kind == api_common.EDataSourceKind_POSTGRESQL {
		opts = request.GetDataSourceInstance().GetPgOptions().GetSchema()
	} else {
		opts = request.GetDataSourceInstance().GetGpOptions().GetSchema()
	}

	query := "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 AND table_schema = $2"
	args := []any{request.Table, opts}

	return query, args
}
