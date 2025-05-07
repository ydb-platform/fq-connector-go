package opensearch

import (
	"github.com/apache/arrow/go/v13/arrow/array"

	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

type Suite struct {
	*suite.Base[string, *array.StringBuilder]
	dataSource *datasource.DataSource
}

func (s *Suite) TestDescribeTable() {
	testCaseNames := []string{"simple", "list", "nested", "nested_list", "optional"}

	for _, testCase := range testCaseNames {
		s.ValidateTableMetadata(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestReadSplitPrimitives() {
	testCaseNames := []string{"simple", "nested", "optional"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

func NewSuite(
	baseSuite *suite.Base[string, *array.StringBuilder],
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &Suite{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}
