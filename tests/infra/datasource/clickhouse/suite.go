package clickhouse

import (
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

type Suite struct {
	*suite.Base
	dataSource *datasource.DataSource
}

func (s *Suite) TestSimpleSelect() {
	for _, dsi := range s.dataSource.Instances {
		for tableName, table := range tables {
			s.ReadTable(tableName, table, dsi)
		}
	}
}

func NewSuite(
	baseSuite *suite.Base,
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	require.NoError(baseSuite.T(), err)

	result := &Suite{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}
