package mongodb

import (
	"github.com/apache/arrow/go/v13/arrow/array"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

type Suite struct {
	*suite.Base[int32, *array.Int32Builder]
	dataSource   *datasource.DataSource
	yqlTypeToUse config.TMongoDbConfig_ObjectIdYqlType
}

func (s *Suite) TestReadSplitPrimitives() {
	if s.yqlTypeToUse != config.TMongoDbConfig_OBJECT_ID_AS_STRING {
		s.T().Skip("Skipping test with ObjectId not YQL String")
	}

	for _, instance := range s.dataSource.Instances {
		instance.Options = defaultMongoDbOptions
	}

	testCaseNames := []string{"simple", "primitives", "missing", "uneven"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestIncludeUnsupported() {
	if s.yqlTypeToUse != config.TMongoDbConfig_OBJECT_ID_AS_STRING {
		s.T().Skip("Skipping test with ObjectId not YQL String")
	}

	for _, instance := range s.dataSource.Instances {
		instance.Options = stringifyMongoDbOptions
	}

	testCaseNames := []string{"unsupported"}
	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestObjectIdAsTaggedString() {
	if s.yqlTypeToUse != config.TMongoDbConfig_OBJECT_ID_AS_TAGGED_STRING {
		s.T().Skip("Skipping test with ObjectId not YQL Tagged<String>")
	}

	for _, instance := range s.dataSource.Instances {
		instance.Options = mongoDbOptionsWithTaggedType
	}

	testCaseNames := []string{"tagged"}
	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

func NewSuite(
	baseSuite *suite.Base[int32, *array.Int32Builder],
	yqlTypeToUse config.TMongoDbConfig_ObjectIdYqlType,
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &Suite{
		Base:         baseSuite,
		dataSource:   ds,
		yqlTypeToUse: yqlTypeToUse,
	}

	return result
}
