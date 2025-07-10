package paging

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

var _ ColumnarBuffer[any] = (*columnarBufferArrowIPCStreamingRows[any])(nil)

// columnarBufferArrowIPCStreamingRows is a specialized implementation for row-based data
// that builds Arrow arrays from individual rows
type columnarBufferArrowIPCStreamingRows[T Acceptor] struct {
	arrowAllocator memory.Allocator
	builders       []array.Builder
	schema         *arrow.Schema
	logger         *zap.Logger
}

// addRow saves a row obtained from the datasource into the buffer
func (cb *columnarBufferArrowIPCStreamingRows[T]) addRow(transformer RowTransformer[T]) error {
	if err := transformer.AppendToArrowBuilders(cb.schema, cb.builders); err != nil {
		return fmt.Errorf("append values to arrow builders: %w", err)
	}

	return nil
}

// addArrowRecord is not the primary method for this implementation
// It returns an error since this implementation is optimized for row-based data
func (cb *columnarBufferArrowIPCStreamingRows[T]) addArrowRecord(record arrow.Record) error {
	return fmt.Errorf("this implementation is optimized for row-based data, use columnarBufferArrowIPCStreamingRecords for Arrow records")
}

// ToResponse returns all the accumulated data and clears buffer
func (cb *columnarBufferArrowIPCStreamingRows[T]) ToResponse() (*api_service_protos.TReadSplitsResponse, error) {
	// If no rows were added, return an empty response
	if cb.TotalRows() == 0 {
		return &api_service_protos.TReadSplitsResponse{}, nil
	}

	// chunk consists of columns
	chunk := make([]arrow.Array, 0, len(cb.builders))

	// prepare arrow record
	for _, builder := range cb.builders {
		chunk = append(chunk, builder.NewArray())
	}

	record := array.NewRecord(cb.schema, chunk, -1)

	// We need to release the arrays after creating the record
	for _, col := range chunk {
		col.Release()
	}

	// prepare arrow writer
	var buf bytes.Buffer

	writer := ipc.NewWriter(&buf, ipc.WithSchema(cb.schema), ipc.WithAllocator(cb.arrowAllocator))

	if err := writer.Write(record); err != nil {
		record.Release()
		return nil, fmt.Errorf("write record: %w", err)
	}

	if err := writer.Close(); err != nil {
		record.Release()
		return nil, fmt.Errorf("close arrow writer: %w", err)
	}

	// Release the record after writing
	record.Release()

	out := &api_service_protos.TReadSplitsResponse{
		Payload: &api_service_protos.TReadSplitsResponse_ArrowIpcStreaming{
			ArrowIpcStreaming: buf.Bytes(),
		},
	}

	return out, nil
}

// TotalRows returns the number of rows in the buffer
func (cb *columnarBufferArrowIPCStreamingRows[T]) TotalRows() int {
	if len(cb.builders) == 0 {
		return 0
	}
	return cb.builders[0].Len()
}

// Release frees resources if buffer is no longer used
func (cb *columnarBufferArrowIPCStreamingRows[T]) Release() {
	// cleanup builders
	for _, b := range cb.builders {
		b.Release()
	}
}
