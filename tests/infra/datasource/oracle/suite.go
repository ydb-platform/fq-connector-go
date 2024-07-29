package oracle

import (
	"context"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"golang.org/x/exp/constraints"

	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

type Suite struct {
	*suite.Base[int64, array.Int64Builder]
	dataSource *datasource.DataSource
}

func (b *Suite) ValidateTable(ds *datasource.DataSource, table *test_utils.Table[int64, array.Int64Builder], customOptions ...suite.ValidateTableOption) {
	for _, dsi := range ds.Instances {
		b.doValidateTable(table, dsi, customOptions...)
	}
}

func (b *Suite) doValidateTable(table *test_utils.Table[int64, array.Int64Builder], dsi *api_common.TDataSourceInstance, customOptions ...suite.ValidateTableOption) {
	options := suite.NewDefaultValidateTableOptions()
	for _, option := range customOptions {
		option.Apply(options)
	}

	b.Require().NotEmpty(table.Name)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// describe table
	describeTableResponse, err := b.Connector.ClientBuffering().DescribeTable(ctx, dsi, options.TypeMappingSettings, table.Name)
	b.Require().NoError(err)
	b.Require().Equal(Ydb.StatusIds_SUCCESS, describeTableResponse.Error.Status, describeTableResponse.Error.String())

	// verify schema
	schema := describeTableResponse.Schema
	table.MatchSchema(b.T(), schema)

	// list splits
	slct := &api_service_protos.TSelect{
		DataSourceInstance: dsi,
		What:               common.SchemaToSelectWhatItems(schema, nil),
		From: &api_service_protos.TSelect_TFrom{
			Table: table.Name,
		},
	}

	if options.Predicate != nil {
		slct.Where = &api_service_protos.TSelect_TWhere{
			FilterTyped: options.Predicate,
		}
	}

	listSplitsResponses, err := b.Connector.ClientBuffering().ListSplits(ctx, slct)
	b.Require().NoError(err)
	b.Require().Len(listSplitsResponses, 1)

	// read splits
	splits := common.ListSplitsResponsesToSplits(listSplitsResponses)
	readSplitsResponses, err := b.Connector.ClientBuffering().ReadSplits(ctx, splits)
	b.Require().NoError(err)
	b.Require().Len(readSplitsResponses, 1)

	records, err := common.ReadResponsesToArrowRecords(readSplitsResponses)
	b.Require().NoError(err)

	// verify data
	table.MatchRecords(b.T(), records, schema)
}

func (s *Suite) TestSelect() {
	testCaseNames := []string{"simple", "long_table", "longraw"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestDatetimeFormatYQL() {
	s.ValidateTable(
		s.dataSource,
		tables["datetime_format_yql"],
		suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_YQL_FORMAT),
	)
}

func (s *Suite) TestDatetimeFormatString() {
	s.ValidateTable(
		s.dataSource,
		tables["datetime_format_string"],
		suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_STRING_FORMAT),
	)
}

func (s *Suite) TestPositiveStats() {
	suite.TestPositiveStats(s.Base, s.dataSource, tables["simple"])
}

func (s *Suite) TestMissingDataSource() {
	dsi := &api_common.TDataSourceInstance{
		Kind:     api_common.EDataSourceKind_ORACLE,
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
		Options: &api_common.TDataSourceInstance_OraOptions{
			OraOptions: &api_common.TOracleDataSourceOptions{
				ServiceName: "it's not important",
			},
		},
		UseTls:   false,
		Protocol: api_common.EProtocol_NATIVE,
	}

	suite.TestMissingDataSource(s.Base, dsi)
}

func (s *Suite) TestInvalidLogin() {
	s.T().Skip()

	for _, dsi := range s.dataSource.Instances {
		suite.TestInvalidLogin(s.Base, dsi, tables["simple"])
	}
}

func (s *Suite) TestInvalidPassword() {
	for _, dsi := range s.dataSource.Instances {
		suite.TestInvalidPassword(s.Base, dsi, tables["simple"])
	}
}

func (s *Suite) TestInvalidServiceName() {
	// for _, dsi := range s.dataSource.Instances {
	// TODO
	// }
}

func NewSuite[T constraints.Integer, K array.Int64Builder | array.Int32Builder](
	baseSuite *suite.Base[int64, array.Int64Builder],
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &Suite{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}
