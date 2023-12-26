package tests

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource/clickhouse"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

type ClickHouseSuite struct {
	*suite.Base
	protocols  []api_common.EProtocol
	dataSource *clickhouse.DataSource
}

func (s *ClickHouseSuite) TestSimpleTable() {
	for _, protocol := range s.protocols {
		s.doTestSimpleTable(protocol)
	}
}

func (s *ClickHouseSuite) doTestSimpleTable(protocol api_common.EProtocol) {
	dsi, err := s.dataSource.GetDataSourceInstance(protocol)
	s.Require().NoError(err)

	// TODO: read splits
	s.describeTable(dsi)
}

func (s *ClickHouseSuite) describeTable(dsi *api_common.TDataSourceInstance) {
	request := &api_service_protos.TDescribeTableRequest{
		DataSourceInstance: dsi,
		Table:              "simple",
	}

	// Describe table
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	response, err := s.Connector.Client().DescribeTable(ctx, request)
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_SUCCESS, response.Error.Status)

	schema := &api_service_protos.TSchema{
		Columns: []*Ydb.Column{
			{
				Name: "id",
				Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
			},
			{
				Name: "col1",
				Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}},
			},
			{
				Name: "col2",
				Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
			},
		},
	}
	s.Require().True(proto.Equal(schema, response.Schema))
}

func NewClickHouseSuite(baseSuite *suite.Base) *ClickHouseSuite {
	result := &ClickHouseSuite{
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
