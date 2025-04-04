package oracle

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

func testInvalidServiceName[ID test_utils.TableIDTypes, IDBUILDER test_utils.ArrowIDBuilder[ID]](
	s *suite.Base[ID, IDBUILDER],
	dsiSrc *api_common.TGenericDataSourceInstance,
	table *test_utils.Table[ID, IDBUILDER],
) {
	dsi := proto.Clone(dsiSrc).(*api_common.TGenericDataSourceInstance)
	oraOpts := dsi.Options.(*api_common.TGenericDataSourceInstance_OracleOptions)

	oraOpts.OracleOptions.ServiceName = "wrong"

	// read some table to "heat" metrics
	resp, err := s.Connector.ClientBuffering().DescribeTable(context.Background(), dsi, nil, table.Name)
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_NOT_FOUND, resp.Error.Status, resp.Error.String())

	// get stats snapshot before table reading
	snapshot1, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// read some table
	resp, err = s.Connector.ClientBuffering().DescribeTable(context.Background(), dsi, nil, table.Name)
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_NOT_FOUND, resp.Error.Status)

	// get stats snapshot after table reading
	snapshot2, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// errors count incremented by one
	describeTableStatusErr, err := common.DiffStatusSensors(snapshot1, snapshot2, "RATE", "DescribeTable", "status_total", "NOT_FOUND")
	s.Require().NoError(err)
	s.Require().Equal(float64(1), describeTableStatusErr)
}
