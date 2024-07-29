package suite

import (
	"context"
	"time"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	test_utils_oracle "github.com/ydb-platform/fq-connector-go/tests/infra/datasource/oracle/utils"
	"github.com/ydb-platform/fq-connector-go/tests/suite"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type SuiteOracle struct {
	*suite.Base
}

func (b *SuiteOracle) ValidateTable(ds *datasource.DataSource, table *test_utils_oracle.Table, customOptions ...suite.ValidateTableOption) {
	for _, dsi := range ds.Instances {
		b.doValidateTable(table, dsi, customOptions...)
	}
}

func (b *SuiteOracle) doValidateTable(table *test_utils_oracle.Table, dsi *api_common.TDataSourceInstance, customOptions ...suite.ValidateTableOption) {
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

func NewOracleSuite(b *suite.Base) *SuiteOracle {
	s := &SuiteOracle{
		Base: b,
	}

	return s
}
