package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/clickhouse"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

// Basic Connector functions:
// * data and metadata extraction
// * working with primitive and optional types
type SelectSuite struct {
	*suite.Base
	protocols  []api_common.EProtocol
	dataSource *datasource.DataSource
}

func (s *SelectSuite) TestSimpleTable() {
	for tableName, table := range clickhouse.Tables {
		for _, protocol := range s.protocols {
			s.doTestSimpleTable(tableName, table, protocol)
		}
	}
}

func (s *SelectSuite) doTestSimpleTable(tableName string, table *datasource.Table, protocol api_common.EProtocol) {
	dsi, err := s.dataSource.GetDataSourceInstance(protocol)
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// TODO: parametrize test
	typeMappingSettings := &api_service_protos.TTypeMappingSettings{
		DateTimeFormat: api_service_protos.EDateTimeFormat_YQL_FORMAT,
	}

	// describe table
	describeTableResponse, err := s.Connector.Client().DescribeTable(ctx, dsi, typeMappingSettings, tableName)
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_SUCCESS, describeTableResponse.Error.Status)
	s.Require().True(
		proto.Equal(table.SchemaYdb, describeTableResponse.Schema),
		fmt.Sprintf("expected: %v\nactual:   %v\n", table.SchemaYdb, describeTableResponse.Schema),
	)

	// list splits
	slct := &api_service_protos.TSelect{
		DataSourceInstance: dsi,
		What:               common.SchemaToSelectWhatItems(table.SchemaYdb, nil),
		From: &api_service_protos.TSelect_TFrom{
			Table: tableName,
		},
	}

	listSplitsResponses, err := s.Connector.Client().ListSplits(ctx, slct)
	s.Require().NoError(err)
	s.Require().Len(listSplitsResponses, 1)

	// read splits
	splits := common.ListSplitsResponsesToSplits(listSplitsResponses)
	readSplitsResponses, err := s.Connector.Client().ReadSplits(ctx, splits)
	s.Require().NoError(err)
	s.Require().Len(readSplitsResponses, 1)

	records, err := common.ReadResponsesToArrowRecords(readSplitsResponses)
	s.Require().NoError(err)

	table.MatchRecords(s.T(), records)
}

func NewSelectSuite(baseSuite *suite.Base, dataSource *datasource.DataSource) *SelectSuite {
	result := &SelectSuite{
		Base: baseSuite,
		protocols: []api_common.EProtocol{
			api_common.EProtocol_HTTP,
			api_common.EProtocol_NATIVE,
		},
		dataSource: dataSource,
	}

	return result
}
