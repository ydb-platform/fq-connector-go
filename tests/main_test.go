package tests

import (
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	testify_suite "github.com/stretchr/testify/suite"

	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/clickhouse"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/postgresql"
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

	// Pause to let datasources start
	// TODO: ping ports
	time.Sleep(3 * time.Second)

	m.Run()
}

func TestSelectClickHouse(t *testing.T) {
	ds, err := clickhouse.DeriveDataSourceFromDockerCompose(state.EndpointDeterminer)
	require.NoError(t, err)

	testify_suite.Run(
		t,
		NewSelectSuite(
			suite.NewBase(t, state, "SelectClickHouse"),
			ds,
			clickhouse.Tables),
	)
}

func TestSelectPostgreSQL(t *testing.T) {
	ds, err := postgresql.DeriveDataSourceFromDockerCompose(state.EndpointDeterminer)
	require.NoError(t, err)

	testify_suite.Run(
		t,
		NewSelectSuite(
			suite.NewBase(t, state, "SelectPostgreSQL"),
			ds,
			postgresql.Tables),
	)
}
