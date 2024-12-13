package common

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

type clientBasic struct {
	client api_service.ConnectorClient
	conn   *grpc.ClientConn
	logger *zap.Logger
}

func (c *clientBasic) DescribeTable(
	ctx context.Context,
	dsi *api_common.TGenericDataSourceInstance,
	typeMappingSettings *api_service_protos.TTypeMappingSettings,
	tableName string,
) (*api_service_protos.TDescribeTableResponse, error) {
	request := &api_service_protos.TDescribeTableRequest{
		DataSourceInstance:  dsi,
		Table:               tableName,
		TypeMappingSettings: typeMappingSettings,
	}

	return c.client.DescribeTable(ctx, request)
}

type ReadSplitsOption interface {
	apply(request *api_service_protos.TReadSplitsRequest)
}

type readSplitsFilteringOption struct {
	filtering api_service_protos.TReadSplitsRequest_EFiltering
}

func (o readSplitsFilteringOption) apply(request *api_service_protos.TReadSplitsRequest) {
	request.Filtering = o.filtering
}

func WithFiltering(filtering api_service_protos.TReadSplitsRequest_EFiltering) ReadSplitsOption {
	return readSplitsFilteringOption{filtering: filtering}
}

func (c *clientBasic) Close() {
	LogCloserError(c.logger, c.conn, "client GRPC connection")
}
