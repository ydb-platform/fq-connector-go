package common

import (
	"context"
	"fmt"
	"io"
	"sync"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
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

type ClientStreaming interface {
	clientBasic

	ListSplits(
		ctx context.Context,
		slct *api_service_protos.TSelect,
	) (<-chan *StreamResult[*api_service_protos.TListSplitsResponse], error)

	ReadSplits(
		ctx context.Context,
		splits []*api_service_protos.TSplit,
	) (<-chan *StreamResult[*api_service_protos.TReadSplitsResponse], error)
}

var _ ClientStreaming = (*clientStreamingImpl)(nil)

type clientStreamingImpl struct {
	clientBasicImpl

	wg       *sync.WaitGroup
	exitChan chan struct{}
}

func (c *clientStreamingImpl) ListSplits(
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

	go streamToChannel[*api_service_protos.TListSplitsResponse](rcvStream, out, c.wg, c.exitChan)

	return out, nil
}

func (c *clientStreamingImpl) ReadSplits(
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

	go streamToChannel[*api_service_protos.TReadSplitsResponse](rcvStream, out, c.wg, c.exitChan)

	return out, nil
}

func streamToChannel[T StreamResponse](in stream[T], out chan<- *StreamResult[T], wg *sync.WaitGroup, exitChan <-chan struct{}) {
	defer wg.Done()

	for {
		var (
			result   *StreamResult[T]
			finished bool
		)

		msg, err := in.Recv()
		if err != nil {
			if err != io.EOF {
				result = &StreamResult[T]{Response: nil, Err: fmt.Errorf("stream recv: %w", err)}
			}

			close(out)

			finished = true
		} else {
			result = &StreamResult[T]{Response: msg, Err: nil}
		}

		select {
		case <-exitChan:
			return
		case out <- result:
		}

		if finished {
			return
		}
	}
}

func (c *clientStreamingImpl) Close() {
	c.wg.Wait()
	c.clientBasicImpl.Close()
}
