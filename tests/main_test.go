package tests

import (
	"log"
	"testing"

	"github.com/apache/arrow/go/v13/arrow/array"
	testify_suite "github.com/stretchr/testify/suite"

	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/ms_sql_server"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

// TODO: find the way of passing this object into suites as a parameter instead of global var
var state *suite.State

func TestMain(m *testing.M) {
	var err error

	state, err = suite.NewState()
	if err != nil {
		log.Fatal(err)
	}

	m.Run()
}

// func TestClickHouse(t *testing.T) {
// 	testify_suite.Run(t, clickhouse.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "ClickHouse")))
// }

// func TestPostgreSQL(t *testing.T) {
// 	testify_suite.Run(t, postgresql.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "PostgreSQL")))
// }

// func TestYDB(t *testing.T) {
// 	testify_suite.Run(t, ydb.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "YDB")))
// }

// func TestGreenplum(t *testing.T) {
// 	testify_suite.Run(t, greenplum.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "Greenplum")))
// }

// func TestMySQL(t *testing.T) {
// 	testify_suite.Run(t, mysql.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "MySQL")))
// }

// func TestOracle(t *testing.T) {
// 	testify_suite.Run(t, oracle.NewSuite(suite.NewBase[int64, *array.Int64Builder](t, state, "Oracle")))
// }

func TestMsSqlServer(t *testing.T) {
	testify_suite.Run(t, ms_sql_server.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "MS SQL Server")))
}
