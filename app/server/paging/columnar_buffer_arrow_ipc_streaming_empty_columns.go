package paging

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

var _ ColumnarBuffer[any] = (*columnarBufferArrowIPCStreamingEmptyColumns[any])(nil)

// special implementation for buffer that writes schema with empty columns set
type columnarBufferArrowIPCStreamingEmptyColumns[T Acceptor] struct {
	arrowAllocator memory.Allocator
	schema         *arrow.Schema
	rowsAdded      int
}

// AddRow saves a row obtained from the datasource into the buffer
//
//nolint:unused
func (cb *columnarBufferArrowIPCStreamingEmptyColumns[T]) addRow(transformer RowTransformer[T]) error {
	if len(transformer.GetAcceptors()) != 1 {
		return fmt.Errorf("expected 1 value, got %v", len(transformer.GetAcceptors()))
	}

	cb.rowsAdded++

	return nil
}

// ToResponse returns all the accumulated data and clears buffer
func (cb *columnarBufferArrowIPCStreamingEmptyColumns[T]) ToResponse() (*api_service_protos.TReadSplitsResponse, error) {
	columns := make([]arrow.Array, 0)

	record := array.NewRecord(cb.schema, columns, int64(cb.rowsAdded))

	// prepare arrow writer
	var buf bytes.Buffer

	writer := ipc.NewWriter(&buf, ipc.WithSchema(cb.schema), ipc.WithAllocator(cb.arrowAllocator))

	if err := writer.Write(record); err != nil {
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

func (cb *columnarBufferArrowIPCStreamingEmptyColumns[T]) TotalRows() int { return cb.rowsAdded }

// Frees resources if buffer is no longer used
func (*columnarBufferArrowIPCStreamingEmptyColumns[T]) Release() {
}
