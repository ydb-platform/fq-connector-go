package connector

import (
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
)

type Client interface {
	DescribeTable(
		ctx context.Context,
		dsi *api_common.TDataSourceInstance,
		tableName string,
	) (*api_service_protos.TDescribeTableResponse, error)

	ListSplits(
		ctx context.Context,
		slct *api_service_protos.TSelect,
	) ([]*api_service_protos.TListSplitsResponse, error)

	ReadSplits(
		ctx context.Context,
		splits []*api_service_protos.TSplit,
	) ([]*api_service_protos.TReadSplitsResponse, error)

	stop()
}

type clientImpl struct {
	client api_service.ConnectorClient
	conn   *grpc.ClientConn
	logger *zap.Logger
}

func (c *clientImpl) DescribeTable(
	ctx context.Context,
	dsi *api_common.TDataSourceInstance,
	tableName string,
) (*api_service_protos.TDescribeTableResponse, error) {
	request := &api_service_protos.TDescribeTableRequest{
		DataSourceInstance: dsi,
		Table:              tableName,
	}

	return c.client.DescribeTable(ctx, request)
}

func (c *clientImpl) ListSplits(
	ctx context.Context,
	slct *api_service_protos.TSelect,
) ([]*api_service_protos.TListSplitsResponse, error) {
	request := &api_service_protos.TListSplitsRequest{
		Selects: []*api_service_protos.TSelect{slct},
	}

	rcvStream, err := c.client.ListSplits(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("list splits: %w", err)
	}

	return dumpStream[*api_service_protos.TListSplitsResponse](rcvStream)
}

func (c *clientImpl) ReadSplits(
	ctx context.Context,
	splits []*api_service_protos.TSplit,
) ([]*api_service_protos.TReadSplitsResponse, error) {
	request := &api_service_protos.TReadSplitsRequest{
		Splits: splits,
		Format: api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING,
	}

	rcvStream, err := c.client.ReadSplits(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("list splits: %w", err)
	}

	return dumpStream[*api_service_protos.TReadSplitsResponse](rcvStream)
}

type StreamResponse interface {
	*api_service_protos.TListSplitsResponse | *api_service_protos.TReadSplitsResponse

	GetError() *api_service_protos.TError
}

type stream[T StreamResponse] interface {
	Recv() (T, error)
}

func dumpStream[T StreamResponse](rcvStream stream[T]) ([]T, error) {
	var responses []T

	for {
		msg, err := rcvStream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("stream recv: %w", err)
		}

		responses = append(responses, msg)
	}

	return responses, nil
}

func (c *clientImpl) stop() {
	common.LogCloserError(c.logger, c.conn, "client GRPC connection")
}

func newClient(logger *zap.Logger, cfg *config.TServerConfig) (Client, error) {
	conn, err := grpc.Dial(
		common.EndpointToString(cfg.ConnectorServer.Endpoint),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	grpcClient := api_service.NewConnectorClient(conn)

	return &clientImpl{client: grpcClient, conn: conn, logger: logger}, nil
}
