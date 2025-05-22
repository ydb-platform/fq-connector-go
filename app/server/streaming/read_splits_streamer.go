package streaming

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
)

type ReadSplitsStreamer[T paging.Acceptor] struct {
	stream      api_service.Connector_ReadSplitsServer
	dataSource  datasource.DataSource[T]
	request     *api_service_protos.TReadSplitsRequest
	split       *api_service_protos.TSplit
	sinkFactory paging.SinkFactory[T]
	queryID     uint64
	logger      *zap.Logger
	errorChan   chan error      // notifies about errors happened during reading process
	ctx         context.Context // clone of a stream context
	cancel      context.CancelFunc
}

func (s *ReadSplitsStreamer[T]) writeDataToStream() error {
	// exit from this function will cause publisher's goroutine termination as well
	defer s.cancel()

	for {
		select {
		case result, ok := <-s.sinkFactory.ResultQueue():
			if !ok {
				// correct termination
				return nil
			}

			if result.Error != nil {
				return fmt.Errorf("read result: %w", result.Error)
			}

			// handle next data block
			if err := s.sendResultToStream(result); err != nil {
				return fmt.Errorf("send buffer to stream: %w", err)
			}

		case err, ok := <-s.errorChan:
			if !ok {
				// error channel was closed, but no error was sent
				return nil
			}

			return fmt.Errorf("read split: %w", err)

		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *ReadSplitsStreamer[T]) sendResultToStream(result *paging.ReadResult[T]) error {
	response, err := result.ColumnarBuffer.ToResponse()
	if err != nil {
		return fmt.Errorf("convert to response: %w", err)
	}

	if err := s.stream.Send(response); err != nil {
		return fmt.Errorf("stream send: %w", err)
	}

	return nil
}

func (s *ReadSplitsStreamer[T]) readDataFromSource() {
	defer close(s.errorChan)

	err := s.dataSource.ReadSplit(
		s.ctx,
		s.logger,
		s.queryID,
		s.request,
		s.split,
		s.sinkFactory,
	)
	if err != nil {
		s.errorChan <- fmt.Errorf("read split: %w", err)
	}
}

func (s *ReadSplitsStreamer[T]) Run() error {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.readDataFromSource()
	}()

	err := s.writeDataToStream()

	// wait for publisher to finish
	wg.Wait()

	return err
}

func NewReadSplitsStreamer[T paging.Acceptor](
	logger *zap.Logger,
	queryID uint64,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sinkFactory paging.SinkFactory[T],
	dataSource datasource.DataSource[T],
) *ReadSplitsStreamer[T] {
	ctx, cancel := context.WithCancel(stream.Context())

	return &ReadSplitsStreamer[T]{
		logger:      logger,
		request:     request,
		stream:      stream,
		split:       split,
		sinkFactory: sinkFactory,
		dataSource:  dataSource,
		queryID:     queryID,
		errorChan:   make(chan error, 1),
		ctx:         ctx,
		cancel:      cancel,
	}
}
