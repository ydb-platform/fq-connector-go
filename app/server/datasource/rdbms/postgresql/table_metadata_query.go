package postgresql

import (
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

func TableMetadataQuery(
	tableName string,
	schema string,
) (string, *rdbms_utils.QueryArgs) {
	query := "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 AND table_schema = $2"

	var args rdbms_utils.QueryArgs

	args.AddUntyped(tableName)
	args.AddUntyped(schema)

	return query, &args
}
