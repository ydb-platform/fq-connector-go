//go:generate stringer -type=sinkState -output=sink_string.go
package paging

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/ipc"
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
	terminateChan  chan<- Sink[T]           // notify factory when the data reading is finished via this channel
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

// AddArrowRecord saves the Arrow block obtained from a stream incoming from an external data source.
// It directly flushes the record to the resultQueue without using the columnar buffer.
func (s *sinkImpl[T]) AddArrowRecord(record arrow.Record) error {
	if s.state != sinkOperational {
		panic(s.unexpectedState(sinkOperational))
	}

	if record == nil {
		return nil
	}

	if record.NumRows() == 0 {
		return nil
	}

	// Apply read limiter for each row in the record
	rowCount := record.NumRows()
	for i := int64(0); i < rowCount; i++ {
		if err := s.readLimiter.addRow(); err != nil {
			return fmt.Errorf("add row to read limiter: %w", err)
		}
	}

	// Check if we can add the Arrow record without exceeding page size limit
	ok, err := s.trafficTracker.tryAddArrowRecord(record)
	if err != nil {
		return fmt.Errorf("add arrow record to traffic tracker: %w", err)
	}

	// If page is already too large, flush buffer to the channel and try again
	if !ok {
		if err := s.flush(true, false); err != nil {
			return fmt.Errorf("flush: %w", err)
		}

		// Try again with a fresh buffer
		_, err := s.trafficTracker.tryAddArrowRecord(record)
		if err != nil {
			return fmt.Errorf("add arrow record to traffic tracker: %w", err)
		}
	}

	// Get stats
	stats := s.trafficTracker.DumpStats(false)

	// Send the response directly to the result queue
	s.respondWithArrowRecord(record, stats, nil, false)

	// Reset counters for the next record
	s.trafficTracker.refreshCounters()

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
	case s.terminateChan <- s:
	case <-s.ctx.Done():
	}
}

func (s *sinkImpl[T]) respondWith(
	buf ColumnarBuffer[T],
	stats *api_service_protos.TReadSplitsResponse_TStats,
	err error,
	isTerminalMessage bool) {
	result := &ReadResult[T]{
		ColumnarBuffer:    buf,
		Stats:             stats,
		Error:             err,
		IsTerminalMessage: isTerminalMessage,
		Logger:            s.logger,
	}

	select {
	case s.resultQueue <- result:
	case <-s.ctx.Done():
	}
}

// respondWithArrowRecord creates a response with an Arrow record and sends it to the result queue
func (s *sinkImpl[T]) respondWithArrowRecord(
	record arrow.Record,
	stats *api_service_protos.TReadSplitsResponse_TStats,
	err error,
	isTerminalMessage bool) {
	// Create a response directly from the Arrow record
	var buf bytes.Buffer

	writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))

	if writeErr := writer.Write(record); writeErr != nil {
		s.respondWith(nil, stats, fmt.Errorf("write record: %w", writeErr), isTerminalMessage)

		return
	}

	if closeErr := writer.Close(); closeErr != nil {
		s.respondWith(nil, stats, fmt.Errorf("close arrow writer: %w", closeErr), isTerminalMessage)

		return
	}

	// Get the serialized data from the buffer
	serializedData := buf.Bytes()

	// Create a result with the serialized data
	result := &ReadResult[T]{
		ColumnarBuffer:    nil,
		Data:              serializedData,
		Stats:             stats,
		Error:             err,
		IsTerminalMessage: isTerminalMessage,
		Logger:            s.logger,
	}

	// Send the result to the queue
	select {
	case s.resultQueue <- result:
	case <-s.ctx.Done():
	}
}

func (s *sinkImpl[T]) unexpectedState(expected ...sinkState) error {
	return fmt.Errorf(
		"unexpected state '%v' (expected are '%v'): %w",
		s.state, expected, common.ErrInvariantViolation)
}

func (s *sinkImpl[T]) Logger() *zap.Logger {
	return s.logger
}
