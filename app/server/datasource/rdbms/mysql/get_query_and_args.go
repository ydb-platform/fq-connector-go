package mysql

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func GetQueryAndArgs(request *api_service_protos.TDescribeTableRequest) (string, []any) {
	// opts := request.GetDataSourceInstance().GetMysqlOptions().GetSchema()
	// TODO: pass table_schema using mysql options. For now fallback to db name, which is default
	// TODO: do not add 'unsigned' and 'nullable' modifiers to column type and use the driver-provided
	// fields instead.
	query := `SELECT column_name, CONCAT(
		IF(COLUMN_TYPE LIKE '%unsigned', CONCAT(DATA_TYPE, ' ', 'unsigned'),
		DATA_TYPE),
		' ',
		IF(IS_NULLABLE = 'YES', 'nullable', '')
	) AS DATA_TYPE FROM information_schema.columns WHERE table_name = ? AND table_schema = ?`

	args := []any{request.Table, request.GetDataSourceInstance().Database}

	return query, args
}
