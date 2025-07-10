package paging

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

var _ ColumnarBuffer[any] = (*columnarBufferArrowIPCStreamingRecords[any])(nil)

// columnarBufferArrowIPCStreamingRecords is a specialized implementation for Arrow-based data
// that stores Arrow records directly without deconstructing them
type columnarBufferArrowIPCStreamingRecords[T Acceptor] struct {
	arrowAllocator memory.Allocator
	schema         *arrow.Schema
	logger         *zap.Logger
	arrowRecord    arrow.Record // Store the Arrow Record directly
}

// addRow is not the primary method for this implementation
// It returns an error since this implementation is optimized for Arrow records
func (cb *columnarBufferArrowIPCStreamingRecords[T]) addRow(transformer RowTransformer[T]) error {
	return fmt.Errorf("this implementation is optimized for Arrow records, use columnarBufferArrowIPCStreamingRows for row-based data")
}

// addArrowRecord saves an Arrow Block obtained from the datasource into the columnar buffer
func (cb *columnarBufferArrowIPCStreamingRecords[T]) addArrowRecord(record arrow.Record) error {
	// Verify schema compatibility
	if !cb.schema.Equal(record.Schema()) {
		return fmt.Errorf("record schema does not match buffer schema")
	}

	// Store the record directly
	if cb.arrowRecord != nil {
		// Release the previous record if it exists
		cb.arrowRecord.Release()
	}

	// Retain the record to prevent it from being garbage collected
	record.Retain()
	cb.arrowRecord = record

	return nil
}

// ToResponse returns all the accumulated data and clears buffer
func (cb *columnarBufferArrowIPCStreamingRecords[T]) ToResponse() (*api_service_protos.TReadSplitsResponse, error) {
	// If no record was added, return an empty response
	if cb.arrowRecord == nil {
		return &api_service_protos.TReadSplitsResponse{}, nil
	}

	// prepare arrow writer
	var buf bytes.Buffer

	writer := ipc.NewWriter(&buf, ipc.WithSchema(cb.schema), ipc.WithAllocator(cb.arrowAllocator))

	if err := writer.Write(cb.arrowRecord); err != nil {
		return nil, fmt.Errorf("write record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close arrow writer: %w", err)
	}

	out := &api_service_protos.TReadSplitsResponse{
		Payload: &api_service_protos.TReadSplitsResponse_ArrowIpcStreaming{
			ArrowIpcStreaming: buf.Bytes(),
		},
	}

	return out, nil
}

// TotalRows returns the number of rows in the buffer
func (cb *columnarBufferArrowIPCStreamingRecords[T]) TotalRows() int {
	if cb.arrowRecord == nil {
		return 0
	}
	return int(cb.arrowRecord.NumRows())
}

// Release frees resources if buffer is no longer used
func (cb *columnarBufferArrowIPCStreamingRecords[T]) Release() {
	// Release the stored Arrow Record if it exists
	if cb.arrowRecord != nil {
		cb.arrowRecord.Release()
		cb.arrowRecord = nil
	}
}
