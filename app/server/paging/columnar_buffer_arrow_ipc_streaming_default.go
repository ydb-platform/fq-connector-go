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

var _ ColumnarBuffer[any] = (*columnarBufferArrowIPCStreamingDefault[any])(nil)

type columnarBufferArrowIPCStreamingDefault[T Acceptor] struct {
	arrowAllocator memory.Allocator
	builders       []array.Builder
	schema         *arrow.Schema
	logger         *zap.Logger
}

// AddRow saves a row obtained from the datasource into the buffer
//
//nolint:unused
func (cb *columnarBufferArrowIPCStreamingDefault[T]) addRow(transformer RowTransformer[T]) error {
	if err := transformer.AppendToArrowBuilders(cb.schema, cb.builders); err != nil {
		return fmt.Errorf("append values to arrow builders: %w", err)
	}

	return nil
}

// ToResponse returns all the accumulated data and clears buffer
func (cb *columnarBufferArrowIPCStreamingDefault[T]) ToResponse() (*api_service_protos.TReadSplitsResponse, error) {
	// chunk consists of columns
	chunk := make([]arrow.Array, 0, len(cb.builders))

	// prepare arrow record
	for _, builder := range cb.builders {
		chunk = append(chunk, builder.NewArray())
	}

	record := array.NewRecord(cb.schema, chunk, -1)

	for _, col := range chunk {
		col.Release()
	}

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

func (cb *columnarBufferArrowIPCStreamingDefault[T]) TotalRows() int { return cb.builders[0].Len() }

// Frees resources if buffer is no longer used
func (cb *columnarBufferArrowIPCStreamingDefault[T]) Release() {
	// cleanup builders
	for _, b := range cb.builders {
		b.Release()
	}
}
