package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

type Client interface {
	DescribeTable(
		ctx context.Context,
		dsi *api_common.TDataSourceInstance,
		typeMappingSettings *api_service_protos.TTypeMappingSettings,
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

	Close()
}

type clientImpl struct {
	client api_service.ConnectorClient
	conn   *grpc.ClientConn
	logger *zap.Logger
}

func (c *clientImpl) DescribeTable(
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

type stream[T common.StreamResponse] interface {
	Recv() (T, error)
}

func dumpStream[T common.StreamResponse](rcvStream stream[T]) ([]T, error) {
	var responses []T

	for {
		msg, err := rcvStream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("stream recv: %w", err)
		}

		if !common.IsSuccess(msg.GetError()) {
			return nil, common.NewSTDErrorFromAPIError(msg.GetError())
		}

		responses = append(responses, msg)
	}

	return responses, nil
}

func (c *clientImpl) Close() {
	common.LogCloserError(c.logger, c.conn, "client GRPC connection")
}

func makeConnection(logger *zap.Logger, cfg *config.TClientConfig) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	if cfg.Tls != nil {
		logger.Info("client will use TLS connections")

		caCrt, err := os.ReadFile(cfg.Tls.Ca)
		if err != nil {
			return nil, err
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCrt) {
			return nil, fmt.Errorf("failed to add server CA's certificate")
		}

		tlsCfg := &tls.Config{
			RootCAs: certPool,
		}

		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		logger.Info("client will use insecure connections")

		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(common.EndpointToString(cfg.Endpoint), opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	return conn, nil
}

func NewClientFromClientConfig(logger *zap.Logger, clientCfg *config.TClientConfig) (Client, error) {
	conn, err := makeConnection(logger, clientCfg)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	grpcClient := api_service.NewConnectorClient(conn)

	return &clientImpl{client: grpcClient, conn: conn, logger: logger}, nil
}

func NewClientFromServerConfig(logger *zap.Logger, serverCfg *config.TServerConfig) (Client, error) {
	clientCfg := &config.TClientConfig{
		Endpoint: serverCfg.ConnectorServer.Endpoint,
	}

	if serverCfg.ConnectorServer.Tls != nil {
		return nil, fmt.Errorf("TLS connections are not implemented yet")
	}

	return NewClientFromClientConfig(logger, clientCfg)
}
