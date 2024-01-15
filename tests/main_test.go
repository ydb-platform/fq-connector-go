package tests

import (
	"log"
	"testing"

	testify_suite "github.com/stretchr/testify/suite"

	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/clickhouse"
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
