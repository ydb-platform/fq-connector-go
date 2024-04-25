package ms_sql_server

import (
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func GetQueryAndArgs(request *api_service_protos.TDescribeTableRequest) (string, []any) {
	// opts := request.GetDataSourceInstance().GetPgOptions().GetSchema()
	fmt.Println("------------GetQueryAndArgs-------------------")
	// fmt.Println(opts)
	fmt.Println("------------------------------------------")
	query := "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'users';"
	// args := []any{request.Table} //, opts}

	return query, nil
}
