package common

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
)

type StreamResponse interface {
	*api_service_protos.TListSplitsResponse | *api_service_protos.TReadSplitsResponse

	GetError() *api_service_protos.TError
}

type stream[T StreamResponse] interface {
	Recv() (T, error)
}

type StreamResult[T StreamResponse] struct {
	Response T
	Err      error
}

type ClientStreaming struct {
	clientBasic

	wg sync.WaitGroup
}

func (c *ClientStreaming) ListSplits(
	ctx context.Context,
	slct *api_service_protos.TSelect,
) (<-chan *StreamResult[*api_service_protos.TListSplitsResponse], error) {
	request := &api_service_protos.TListSplitsRequest{
		Selects: []*api_service_protos.TSelect{slct},
	}

	rcvStream, err := c.client.ListSplits(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("list splits: %w", err)
	}

	out := make(chan *StreamResult[*api_service_protos.TListSplitsResponse])

	c.wg.Add(1)

	go streamToChannel[*api_service_protos.TListSplitsResponse](ctx, rcvStream, out, &c.wg)

	return out, nil
}

func (c *ClientStreaming) ReadSplits(
	ctx context.Context,
	splits []*api_service_protos.TSplit,
) (<-chan *StreamResult[*api_service_protos.TReadSplitsResponse], error) {
	request := &api_service_protos.TReadSplitsRequest{
		Splits: splits,
		Format: api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING,
	}

	rcvStream, err := c.client.ReadSplits(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("list splits: %w", err)
	}

	out := make(chan *StreamResult[*api_service_protos.TReadSplitsResponse])

	c.wg.Add(1)

	go streamToChannel[*api_service_protos.TReadSplitsResponse](ctx, rcvStream, out, &c.wg)

	return out, nil
}

func streamToChannel[T StreamResponse](ctx context.Context, in stream[T], out chan<- *StreamResult[T], wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)

	for {
		msg, err := in.Recv()
		if err != nil {
			if err != io.EOF {
				select {
				case out <- &StreamResult[T]{Response: nil, Err: fmt.Errorf("stream recv: %w", err)}:
				case <-ctx.Done():
					return
				}
			}

			return
		}

		select {
		case out <- &StreamResult[T]{Response: msg, Err: nil}:
		case <-ctx.Done():
			return
		}
	}
}

func (c *ClientStreaming) Close() {
	c.wg.Wait()
	c.clientBasic.Close()
}

func NewClientStreamingFromClientConfig(logger *zap.Logger, clientCfg *config.TClientConfig) (*ClientStreaming, error) {
	conn, err := makeConnection(logger, clientCfg, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(math.MaxInt64),
	))

	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	grpcClient := api_service.NewConnectorClient(conn)

	return &ClientStreaming{
		clientBasic: clientBasic{
			client: grpcClient,
			conn:   conn,
			logger: logger},
	}, nil
}

func NewClientStreamingFromServerConfig(logger *zap.Logger, serverCfg *config.TServerConfig) (*ClientStreaming, error) {
	clientCfg := &config.TClientConfig{
		ConnectorServerEndpoint: serverCfg.ConnectorServer.Endpoint,
	}

	if serverCfg.ConnectorServer.Tls != nil {
		return nil, fmt.Errorf("TLS connections are not implemented yet")
	}

	return NewClientStreamingFromClientConfig(logger, clientCfg)
}
