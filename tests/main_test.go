package tests

import (
	"flag"
	"fmt"
	"log"
	"testing"

	"github.com/apache/arrow/go/v13/arrow/array"
	testify_suite "github.com/stretchr/testify/suite"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/clickhouse"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/greenplum"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/ms_sql_server"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/mysql"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/oracle"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/postgresql"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/ydb"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

// TODO: find the way of passing this object into suites as a parameter instead of global var
var (
	state *suite.State
)

func TestMain(m *testing.M) {
	flag.Parse()

	var err error

	state, err = suite.NewState()
	if err != nil {
		log.Fatal(err)
	}

	m.Run()
}

func TestClickHouse(t *testing.T) {
	state.SkipSuiteIfNotEnabled(t)
	testify_suite.Run(t, clickhouse.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "ClickHouse")))
}

func TestPostgreSQL(t *testing.T) {
	state.SkipSuiteIfNotEnabled(t)
	testify_suite.Run(t, postgresql.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "PostgreSQL")))
}

func TestYDB(t *testing.T) {
	state.SkipSuiteIfNotEnabled(t)
	modes := []config.TYdbConfig_Mode{
		config.TYdbConfig_MODE_TABLE_SERVICE_STDLIB_SCAN_QUERIES,
		config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE,
	}

	for _, mode := range modes {
		suiteName := fmt.Sprintf("YDB_%v", config.TYdbConfig_Mode_name[int32(mode)])
		option := suite.WithEmbeddedOptions(server.WithYdbConnectorMode(mode))

		testify_suite.Run(
			t,
			ydb.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, suiteName, option), mode),
		)
	}
}

func TestGreenplum(t *testing.T) {
	state.SkipSuiteIfNotEnabled(t)
	testify_suite.Run(t, greenplum.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "Greenplum")))
}

func TestMySQL(t *testing.T) {
	state.SkipSuiteIfNotEnabled(t)
	testify_suite.Run(t, mysql.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "MySQL")))
}

func TestOracle(t *testing.T) {
	state.SkipSuiteIfNotEnabled(t)
	testify_suite.Run(t, oracle.NewSuite(suite.NewBase[int64, *array.Int64Builder](t, state, "Oracle")))
}

func TestMsSqlServer(t *testing.T) {
	state.SkipSuiteIfNotEnabled(t)
	testify_suite.Run(t, ms_sql_server.NewSuite(suite.NewBase[int32, *array.Int32Builder](t, state, "MS SQL Server")))
}
