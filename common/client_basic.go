package common

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

type clientBasic interface {
	DescribeTable(
		ctx context.Context,
		dsi *api_common.TDataSourceInstance,
		typeMappingSettings *api_service_protos.TTypeMappingSettings,
		tableName string,
	) (*api_service_protos.TDescribeTableResponse, error)

	Close()
}

var _ clientBasic = (*clientBasicImpl)(nil)

type clientBasicImpl struct {
	client api_service.ConnectorClient
	conn   *grpc.ClientConn
	logger *zap.Logger
}

func (c *clientBasicImpl) DescribeTable(
	ctx context.Context,
	dsi *api_common.TDataSourceInstance,
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

func (c *clientBasicImpl) Close() {
	LogCloserError(c.logger, c.conn, "client GRPC connection")
}
