package oracle

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"golang.org/x/exp/constraints"
	"google.golang.org/protobuf/proto"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/common"

	"github.com/ydb-platform/fq-connector-go/tests/suite"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

func testInvalidServiceName[T constraints.Integer, K test_utils.ArrowIDBuilder[T]](
	s *suite.Base[T, K],
	dsiSrc *api_common.TDataSourceInstance,
	table *test_utils.Table[T, K],
) {
	dsi := proto.Clone(dsiSrc).(*api_common.TDataSourceInstance)
	oraOpts := dsi.Options.(*api_common.TDataSourceInstance_OraOptions)

	oraOpts.OraOptions.ServiceName = "wrong"

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
