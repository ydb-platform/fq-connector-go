package mysql

import (
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

type Suite struct {
	*suite.Base
	dataSource *datasource.DataSource
}

func (s *Suite) TestSelect() {
	testCaseNames := []string{"simple"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

// Set of tests validating stats

func (s *Suite) TestPositiveStats() {
	suite.TestPositiveStats(s.Base, s.dataSource, tables["simple"])
}

func (s *Suite) TestMissingDataSource() {
	dsi := &api_common.TDataSourceInstance{
		Kind:     api_common.EDataSourceKind_MYSQL,
		Endpoint: &api_common.TEndpoint{Host: "missing_data_source", Port: 3306},
		Database: "it's not important",
		Credentials: &api_common.TCredentials{
			Payload: &api_common.TCredentials_Basic{
				Basic: &api_common.TCredentials_TBasic{
					Username: "it's not important",
					Password: "it's not important",
				},
			},
		},
		UseTls:   false,
		Protocol: api_common.EProtocol_NATIVE,
	}

	suite.TestMissingDataSource(s.Base, dsi)
}

// TODO: fix error mapping in `common/errors.go`
func (s *Suite) TestInvalidLogin() {
	for _, dsi := range s.dataSource.Instances {
		suite.TestInvalidLogin(s.Base, dsi, tables["simple"])
	}
}

func (s *Suite) TestInvalidPassword() {
	for _, dsi := range s.dataSource.Instances {
		suite.TestInvalidPassword(s.Base, dsi, tables["simple"])
	}
}

func NewSuite(
	baseSuite *suite.Base,
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &Suite{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}
