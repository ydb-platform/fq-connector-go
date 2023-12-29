package tests

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/tests/infra/connector"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/clickhouse"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

// Basic Connector functions:
// * data and metadata extraction
// * working with primitive and optional types
type SelectSuite struct {
	*suite.Base
	protocols  []api_common.EProtocol
	dataSource *clickhouse.DataSource
}

func (s *SelectSuite) TestSimpleTable() {
	for _, protocol := range s.protocols {
		s.doTestSimpleTable(protocol)
	}
}

func (s *SelectSuite) doTestSimpleTable(protocol api_common.EProtocol) {
	dsi, err := s.dataSource.GetDataSourceInstance(protocol)
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// describe table
	describeTableResponse, err := s.Connector.Client().DescribeTable(ctx, dsi, "simple")
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_SUCCESS, describeTableResponse.Error.Status)
	s.Require().True(proto.Equal(clickhouse.Tables["simple"].Schema, describeTableResponse.Schema))

	// list splits
	slct := &api_service_protos.TSelect{
		DataSourceInstance: dsi,
		What: &api_service_protos.TSelect_TWhat{
			Items: schemaToSelectWhatItems(clickhouse.Tables["simple"].Schema, nil),
		},
		From: &api_service_protos.TSelect_TFrom{
			Table: "simple",
		},
	}

	listSplitsResponses, err := s.Connector.Client().ListSplits(ctx, slct)
	s.Require().NoError(err)
	s.Require().Len(listSplitsResponses, 1)
	s.Require().True(allResponsesSuccessful[*api_service_protos.TListSplitsResponse](listSplitsResponses))

	// read splits

	splits := listSplitsResponsesToSplits(listSplitsResponses)
	readSplitsResponses, err := s.Connector.Client().ReadSplits(ctx, splits)
	s.Require().NoError(err)
	s.Require().Len(readSplitsResponses, 1)
	s.Require().True(allResponsesSuccessful[*api_service_protos.TListSplitsResponse](listSplitsResponses))
}

func schemaToSelectWhatItems(
	schema *api_service_protos.TSchema,
	whitelist map[string]struct{},
) []*api_service_protos.TSelect_TWhat_TItem {
	out := []*api_service_protos.TSelect_TWhat_TItem{}

	for _, column := range schema.Columns {
		pick := true

		if whitelist != nil {
			if _, exists := whitelist[column.Name]; !exists {
				pick = false
			}
		}

		if pick {
			item := &api_service_protos.TSelect_TWhat_TItem{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: column,
				},
			}

			out = append(out, item)
		}
	}

	return out
}

func allResponsesSuccessful[T connector.StreamResponse](responses []T) bool {
	for _, resp := range responses {
		if resp.GetError().Status != Ydb.StatusIds_SUCCESS {
			return false
		}
	}

	return true
}

func listSplitsResponsesToSplits(in []*api_service_protos.TListSplitsResponse) []*api_service_protos.TSplit {
	var out []*api_service_protos.TSplit

	for _, resp := range in {
		out = append(out, resp.Splits...)
	}

	return out
}

func NewSelectSuite(baseSuite *suite.Base) *SelectSuite {
	result := &SelectSuite{
		Base: baseSuite,
		protocols: []api_common.EProtocol{
			api_common.EProtocol_HTTP,
			api_common.EProtocol_NATIVE,
		},
	}

	var err error
	result.dataSource, err = clickhouse.DeriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	return result
}
