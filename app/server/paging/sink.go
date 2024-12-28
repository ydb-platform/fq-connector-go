//go:generate stringer -type=sinkState -output=sink_string.go
package paging

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

type sinkState int8

const (
	sinkOperational sinkState = iota + 1
	sinkFailed
	sinkFinished
)

var _ Sink[any] = (*sinkImpl[any])(nil)
var _ Sink[string] = (*sinkImpl[string])(nil)

type sinkImpl[T Acceptor] struct {
	currBuffer     ColumnarBuffer[T]        // accumulates incoming rows
	resultQueue    chan *ReadResult[T]      // outgoing buffer queue
	terminateChan  chan<- struct{}          // notify factory when the data reading is finished
	bufferFactory  ColumnarBufferFactory[T] // creates new buffer
	trafficTracker *trafficTracker[T]       // tracks the amount of data passed through the sink
	readLimiter    ReadLimiter              // helps to restrict the number of rows read in every request
	logger         *zap.Logger              // annotated logger
	state          sinkState                // flag showing if it's ready to return data
	ctx            context.Context          // client context
}

func (s *sinkImpl[T]) AddRow(rowTransformer RowTransformer[T]) error {
	if s.state != sinkOperational {
		panic(s.unexpectedState(sinkOperational))
	}

	if err := s.readLimiter.addRow(); err != nil {
		return fmt.Errorf("add row to read limiter: %w", err)
	}

	// Check if we can add one more data row
	// without exceeding page size limit.
	ok, err := s.trafficTracker.tryAddRow(rowTransformer.GetAcceptors())
	if err != nil {
		return fmt.Errorf("add row to traffic tracker: %w", err)
	}

	// If page is already too large, flush buffer to the channel and create a new one
	if !ok {
		if err := s.flush(true, false); err != nil {
			return fmt.Errorf("flush: %w", err)
		}

		_, err := s.trafficTracker.tryAddRow(rowTransformer.GetAcceptors())
		if err != nil {
			return fmt.Errorf("add row to traffic tracker: %w", err)
		}
	}

	// Append row data to the columnar buffer
	if err := s.currBuffer.addRow(rowTransformer); err != nil {
		return fmt.Errorf("add row to buffer: %w", err)
	}

	return nil
}

func (s *sinkImpl[T]) flush(makeNewBuffer bool, isTerminalMessage bool) error {
	if s.currBuffer.TotalRows() == 0 {
		return nil
	}

	stats := s.trafficTracker.DumpStats(false)

	// enqueue message to GRPC stream
	s.respondWith(s.currBuffer, stats, nil, isTerminalMessage)

	// create empty buffer and reset counters
	s.currBuffer = nil
	s.trafficTracker.refreshCounters()

	if makeNewBuffer {
		var err error

		s.currBuffer, err = s.bufferFactory.MakeBuffer()
		if err != nil {
			return fmt.Errorf("make buffer: %w", err)
		}
	}

	return nil
}

func (s *sinkImpl[T]) Finish() {
	if s.state != sinkOperational && s.state != sinkFailed {
		panic(s.unexpectedState(sinkOperational, sinkFailed))
	}

	// if there is some data left, send it to the channel
	if s.state == sinkOperational {
		err := s.flush(false, true)
		if err != nil {
			s.respondWith(nil, nil, fmt.Errorf("flush: %w", err), true)
			s.state = sinkFailed
		} else {
			s.state = sinkFinished
		}
	}

	// notify factory about the end of data
	select {
	case s.terminateChan <- struct{}{}:
	case <-s.ctx.Done():
	}
}

func (s *sinkImpl[T]) respondWith(
	buf ColumnarBuffer[T],
	stats *api_service_protos.TReadSplitsResponse_TStats,
	err error,
	isTerminalMessage bool) {
	select {
	case s.resultQueue <- &ReadResult[T]{ColumnarBuffer: buf, Stats: stats, Error: err, IsTerminalMessage: isTerminalMessage}:
	case <-s.ctx.Done():
	}
}

func (s *sinkImpl[T]) unexpectedState(expected ...sinkState) error {
	return fmt.Errorf(
		"unexpected state '%v' (expected are '%v'): %w",
		s.state, expected, common.ErrInvariantViolation)
}
