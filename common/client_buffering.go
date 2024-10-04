package common

import (
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"

	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
)

type ClientBuffering struct {
	clientBasic
}

func (c *ClientBuffering) ListSplits(
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

func (c *ClientBuffering) ReadSplits(
	ctx context.Context,
	splits []*api_service_protos.TSplit,
	options ...ReadSplitsOption,
) ([]*api_service_protos.TReadSplitsResponse, error) {
	request := &api_service_protos.TReadSplitsRequest{
		Splits: splits,
		Format: api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING,
	}

	for _, option := range options {
		option.apply(request)
	}

	rcvStream, err := c.client.ReadSplits(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("list splits: %w", err)
	}

	return dumpStream[*api_service_protos.TReadSplitsResponse](rcvStream)
}

// dumpStream dumps the stream into a slice;
// it also returns transport error as the second argument,
// but you need to check logical errors on your own.
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

func NewClientBufferingFromClientConfig(logger *zap.Logger, clientCfg *config.TClientConfig) (*ClientBuffering, error) {
	conn, err := makeConnection(logger, clientCfg)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	grpcClient := api_service.NewConnectorClient(conn)

	return &ClientBuffering{
		clientBasic: clientBasic{
			client: grpcClient,
			conn:   conn,
			logger: logger,
		},
	}, nil
}

func NewClientBufferingFromServerConfig(logger *zap.Logger, serverCfg *config.TServerConfig) (*ClientBuffering, error) {
	clientCfg := &config.TClientConfig{
		ConnectorServerEndpoint: serverCfg.ConnectorServer.Endpoint,
	}

	if serverCfg.ConnectorServer.Tls != nil {
		return nil, fmt.Errorf("TLS connections are not implemented yet")
	}

	return NewClientBufferingFromClientConfig(logger, clientCfg)
}
