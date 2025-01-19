package mongodb

import (
	"github.com/apache/arrow/go/v13/arrow/array"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

type Suite struct {
	*suite.Base[string, *array.StringBuilder]
	dataSource *datasource.DataSource
}

func (s *Suite) TestDescribeTable() {
	testCaseNames := []string{"simple", "primitives", "missing", "uneven", "nested", "datetime"}

	for _, testCase := range testCaseNames {
		s.ValidateTableMetadata(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestDescribeTableLeaveUnparsed() {
	for _, instance := range s.dataSource.Instances {
		instance.Options = &api_common.TGenericDataSourceInstance_MongodbOptions{
			MongodbOptions: &api_common.TMongoDbDataSourceOptions{
				CountDocsToRead:      3,
				DoParse:              false,
				SkipUnsupportedTypes: true,
			},
		}
	}

	testCaseNames := []string{"simple_json"}
	for _, testCase := range testCaseNames {
		s.ValidateTableMetadata(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestDescribeTableIncludeUnsupported() {
	for _, instance := range s.dataSource.Instances {
		instance.Options = &api_common.TGenericDataSourceInstance_MongodbOptions{
			MongodbOptions: &api_common.TMongoDbDataSourceOptions{
				CountDocsToRead:      3,
				DoParse:              true,
				SkipUnsupportedTypes: false,
			},
		}
	}

	testCaseNames := []string{"unsupported"}
	for _, testCase := range testCaseNames {
		s.ValidateTableMetadata(s.dataSource, tables[testCase])
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
