package clickhouse

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

type Suite struct {
	*suite.Base
	dataSource *datasource.DataSource
}

func (s *Suite) TestSelect() {
	testCaseNames := []string{"simple", "primitives"}

	for _, dsi := range s.dataSource.Instances {
		for _, tableName := range testCaseNames {
			s.ValidateTable(tables[tableName], dsi)
		}
	}
}

func (s *Suite) TestDatetimeFormatYQL() {
	for _, dsi := range s.dataSource.Instances {
		s.ValidateTable(
			tables["datetime_format_yql"],
			dsi,
			suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_YQL_FORMAT),
		)
	}
}

func (s *Suite) TestDatetimeFormatString() {
	for _, dsi := range s.dataSource.Instances {
		s.ValidateTable(
			tables["datetime_format_string"],
			dsi,
			suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_STRING_FORMAT),
		)
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
