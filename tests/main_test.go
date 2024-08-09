package tests

import (
	"log"
	"testing"

	testify_suite "github.com/stretchr/testify/suite"

	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/clickhouse"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/greenplum"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/ms_sql_server"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/mysql"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/postgresql"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/ydb"
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

func TestClickHouse(t *testing.T) {
	testify_suite.Run(t, clickhouse.NewSuite(suite.NewBase(t, state, "ClickHouse")))
}

func TestPostgreSQL(t *testing.T) {
	testify_suite.Run(t, postgresql.NewSuite(suite.NewBase(t, state, "PostgreSQL")))
}

func TestYDB(t *testing.T) {
	testify_suite.Run(t, ydb.NewSuite(suite.NewBase(t, state, "YDB")))
}

func TestGreenplum(t *testing.T) {
	testify_suite.Run(t, greenplum.NewSuite(suite.NewBase(t, state, "Greenplum")))
}

func TestMySQL(t *testing.T) {
	testify_suite.Run(t, mysql.NewSuite(suite.NewBase(t, state, "MySQL")))
}

func TestMsSqlServer(t *testing.T) {
	testify_suite.Run(t, ms_sql_server.NewSuite(suite.NewBase(t, state, "MS SQL Server")))
}
