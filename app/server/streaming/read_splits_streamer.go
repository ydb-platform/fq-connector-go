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
	"github.com/ydb-platform/fq-connector-go/common"
)

type ReadSplitsStreamer[T paging.Acceptor] struct {
	stream      api_service.Connector_ReadSplitsServer
	dataSource  datasource.DataSource[T]
	request     *api_service_protos.TReadSplitsRequest
	split       *api_service_protos.TSplit
	sinkFactory paging.SinkFactory[T]
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
		case err := <-s.errorChan:
			// an error occurred during reading
			if err != nil {
				return fmt.Errorf("error: %w", err)
			}
		case <-s.stream.Context().Done():
			// handle request termination
			return s.stream.Context().Err()
		}
	}
}

func (s *ReadSplitsStreamer[T]) sendResultToStream(result *paging.ReadResult[T]) error {
	// buffer must be explicitly marked as unused,
	// otherwise memory will leak
	defer result.ColumnarBuffer.Release()

	resp, err := result.ColumnarBuffer.ToResponse()
	if err != nil {
		return fmt.Errorf("buffer to response: %w", err)
	}

	resp.Stats = result.Stats

	// if stream is finished, assign successful operation code
	if result.IsTerminalMessage {
		resp.Error = common.NewSuccess()
	}

	dumpReadSplitsResponse(result.Logger, resp)

	if err := s.stream.Send(resp); err != nil {
		return fmt.Errorf("stream send: %w", err)
	}

	return nil
}

func dumpReadSplitsResponse(logger *zap.Logger, resp *api_service_protos.TReadSplitsResponse) {
	switch t := resp.GetPayload().(type) {
	case *api_service_protos.TReadSplitsResponse_ArrowIpcStreaming:
		if dump := resp.GetArrowIpcStreaming(); dump != nil {
			logger.Debug("response",
				zap.Uint64("rows", resp.GetStats().Rows),
				zap.Uint64("bytes", resp.GetStats().Bytes),
				zap.Int("arrow_blob_size", len(dump)))
		}
	case *api_service_protos.TReadSplitsResponse_ColumnSet:
		for i := range t.ColumnSet.Data {
			data := t.ColumnSet.Data[i]
			meta := t.ColumnSet.Meta[i]

			logger.Debug("response", zap.Int("column_id", i), zap.String("meta", meta.String()), zap.String("data", data.String()))
		}
	default:
		panic(fmt.Sprintf("unexpected message type %v", t))
	}
}

func (s *ReadSplitsStreamer[T]) Run() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	defer wg.Wait()

	// Launch reading from the data source.
	// Subscriber goroutine controls publisher goroutine lifetime.
	go func() {
		defer wg.Done()

		select {
		case s.errorChan <- s.dataSource.ReadSplit(s.ctx, s.logger, s.request, s.split, s.sinkFactory):
		case <-s.ctx.Done():
		}
	}()

	// Pass received blocks into the GRPC channel
	if err := s.writeDataToStream(); err != nil {
		return fmt.Errorf("write data to stream: %w", err)
	}

	return nil
}

func NewReadSplitsStreamer[T paging.Acceptor](
	logger *zap.Logger,
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
		dataSource:  dataSource,
		sinkFactory: sinkFactory,
		errorChan:   make(chan error),
		ctx:         ctx,
		cancel:      cancel,
	}
}
