package suite

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
)

func TestPositiveStats(s *Base, dataSource *datasource.DataSource, table *datasource.Table) {
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

func TestMissingDataSource(s *Base, dsi *api_common.TDataSourceInstance) {
	// read some table to "heat" metrics
	resp, err := s.Connector.ClientBuffering().DescribeTable(context.Background(), dsi, nil, "it's not important")
	s.Require().NoError(err)
	s.Require().Equal(Ydb.StatusIds_INTERNAL_ERROR, resp.Error.Status)

	// get stats snapshot before table reading
	snapshot1, err := s.Connector.MetricsSnapshot()
	s.Require().NoError(err)

	// read some table
	resp, err = s.Connector.ClientBuffering().DescribeTable(context.Background(), dsi, nil, "it's not important")
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
