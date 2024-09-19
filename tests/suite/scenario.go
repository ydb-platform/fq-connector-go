package suite

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

func TestPositiveStats[ID test_utils.TableIDTypes, IDBUILDER test_utils.ArrowIDBuilder[ID]](
	s *Base[ID, IDBUILDER],
	dataSource *datasource.DataSource,
	table *test_utils.Table[ID, IDBUILDER],
) {
	// read some table to "heat" metrics
	s.ValidateTable(dataSource, table)

	// get stats snapshot before table reading
	snapshot1, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// read some table
	s.ValidateTable(dataSource, table)

	// get stats snapshot after table reading
	snapshot2, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// Successful status codes incremented by N, where N is a number of data source instances
	describeTableStatusOK, err := common.DiffStatusSensors(snapshot1, snapshot2, "RATE", "DescribeTable", "status_total", "OK")
	s.Require().NoError(err)
	s.Require().Equal(float64(len(dataSource.Instances)), describeTableStatusOK)

	listSplitsStatusOK, err := common.DiffStatusSensors(snapshot1, snapshot2, "RATE", "ListSplits", "stream_status_total", "OK")
	s.Require().NoError(err)
	s.Require().Equal(float64(len(dataSource.Instances)), listSplitsStatusOK)

	readSplitsStatusOK, err := common.DiffStatusSensors(snapshot1, snapshot2, "RATE", "ReadSplits", "stream_status_total", "OK")
	s.Require().NoError(err)
	s.Require().Equal(float64(len(dataSource.Instances)), readSplitsStatusOK)
}

func TestMissingDataSource[
	ID test_utils.TableIDTypes,
	IDBUILDER test_utils.ArrowIDBuilder[ID],
](s *Base[ID, IDBUILDER], dsi *api_common.TDataSourceInstance) {
	// Do not retry negative tests
	md := metadata.Pairs(common.ForbidRetries, "1")
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	// read some table metadata to "heat" metrics
	resp, err := s.Connector.ClientBuffering().DescribeTable(ctx, dsi, nil, "it's not important")
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_INTERNAL_ERROR, resp.Error.Status)

	// get stats snapshot before table reading
	snapshot1, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// read some table metadata
	resp, err = s.Connector.ClientBuffering().DescribeTable(ctx, dsi, nil, "it's not important")
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_INTERNAL_ERROR, resp.Error.Status)

	// get stats snapshot after table reading
	snapshot2, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// errors count incremented by one
	describeTableStatusErr, err := common.DiffStatusSensors(snapshot1, snapshot2, "RATE", "DescribeTable", "status_total", "INTERNAL_ERROR")
	s.Require().NoError(err)
	s.Require().Equal(float64(1), describeTableStatusErr)
}

func TestInvalidLogin[ID test_utils.TableIDTypes, IDBUILDER test_utils.ArrowIDBuilder[ID]](
	s *Base[ID, IDBUILDER],
	dsiSrc *api_common.TDataSourceInstance,
	table *test_utils.Table[ID, IDBUILDER],
) {
	dsi := proto.Clone(dsiSrc).(*api_common.TDataSourceInstance)

	dsi.Credentials.GetBasic().Username = "wrong"

	// read some table to "heat" metrics
	resp, err := s.Connector.ClientBuffering().DescribeTable(context.Background(), dsi, nil, table.Name)
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_UNAUTHORIZED, resp.Error.Status, resp.Error.String())

	// get stats snapshot before table reading
	snapshot1, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// read some table
	resp, err = s.Connector.ClientBuffering().DescribeTable(context.Background(), dsi, nil, table.Name)
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_UNAUTHORIZED, resp.Error.Status)

	// get stats snapshot after table reading
	snapshot2, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// errors count incremented by one
	describeTableStatusErr, err := common.DiffStatusSensors(snapshot1, snapshot2, "RATE", "DescribeTable", "status_total", "UNAUTHORIZED")
	s.Require().NoError(err)
	s.Require().Equal(float64(1), describeTableStatusErr)
}

func TestInvalidPassword[ID test_utils.TableIDTypes, IDBUILDER test_utils.ArrowIDBuilder[ID]](
	s *Base[ID, IDBUILDER],
	dsiSrc *api_common.TDataSourceInstance,
	table *test_utils.Table[ID, IDBUILDER],
) {
	dsi := proto.Clone(dsiSrc).(*api_common.TDataSourceInstance)

	dsi.Credentials.GetBasic().Password = "wrong"

	// read some table to "heat" metrics
	resp, err := s.Connector.ClientBuffering().DescribeTable(context.Background(), dsi, nil, table.Name)
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_UNAUTHORIZED, resp.Error.Status, resp.Error.String())

	// get stats snapshot before table reading
	snapshot1, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// read some table
	resp, err = s.Connector.ClientBuffering().DescribeTable(context.Background(), dsi, nil, table.Name)
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_UNAUTHORIZED, resp.Error.Status)

	// get stats snapshot after table reading
	snapshot2, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// errors count incremented by one
	describeTableStatusErr, err := common.DiffStatusSensors(snapshot1, snapshot2, "RATE", "DescribeTable", "status_total", "UNAUTHORIZED")
	s.Require().NoError(err)
	s.Require().Equal(float64(1), describeTableStatusErr)
}
