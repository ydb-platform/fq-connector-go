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
	arrowRecord    arrow.Record // Store the Arrow Record directly
	rowsAdded      bool         // Track if rows were added via addRow
}

// AddRow saves a row obtained from the datasource into the buffer
//
//nolint:unused
func (cb *columnarBufferArrowIPCStreamingDefault[T]) addRow(transformer RowTransformer[T]) error {
	if err := transformer.AppendToArrowBuilders(cb.schema, cb.builders); err != nil {
		return fmt.Errorf("append values to arrow builders: %w", err)
	}

	cb.rowsAdded = true

	return nil
}

// addArrowRecord saves an Arrow Block obtained from the datasource into the columnar buffer
func (cb *columnarBufferArrowIPCStreamingDefault[T]) addArrowRecord(record arrow.Record) error {
	// Create a new record with the same schema as the buffer
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
func (cb *columnarBufferArrowIPCStreamingDefault[T]) ToResponse() (*api_service_protos.TReadSplitsResponse, error) {
	var record arrow.Record
	var releaseRecord bool

	// If we have a stored Arrow Record, use it directly
	if cb.arrowRecord != nil {
		record = cb.arrowRecord
		// We'll release our reference to the record at the end
		releaseRecord = false
	} else if cb.rowsAdded {
		// If rows were added, create a new record from the builders
		chunk := make([]arrow.Array, 0, len(cb.builders))

		// prepare arrow record
		for _, builder := range cb.builders {
			chunk = append(chunk, builder.NewArray())
		}

		record = array.NewRecord(cb.schema, chunk, -1)

		// We need to release the arrays after creating the record
		for _, col := range chunk {
			col.Release()
		}

		// We'll need to release this record after writing it
		releaseRecord = true
	} else {
		// No data to return
		return &api_service_protos.TReadSplitsResponse{}, nil
	}

	// prepare arrow writer
	var buf bytes.Buffer

	writer := ipc.NewWriter(&buf, ipc.WithSchema(cb.schema), ipc.WithAllocator(cb.arrowAllocator))

	if err := writer.Write(record); err != nil {
		if releaseRecord {
			record.Release()
		}
		return nil, fmt.Errorf("write record: %w", err)
	}

	if err := writer.Close(); err != nil {
		if releaseRecord {
			record.Release()
		}
		return nil, fmt.Errorf("close arrow writer: %w", err)
	}

	// Release the record if we created it
	if releaseRecord {
		record.Release()
	}

	out := &api_service_protos.TReadSplitsResponse{
		Payload: &api_service_protos.TReadSplitsResponse_ArrowIpcStreaming{
			ArrowIpcStreaming: buf.Bytes(),
		},
	}

	return out, nil
}

func (cb *columnarBufferArrowIPCStreamingDefault[T]) TotalRows() int {
	if cb.arrowRecord != nil {
		return int(cb.arrowRecord.NumRows())
	}
	if len(cb.builders) > 0 {
		return cb.builders[0].Len()
	}
	return 0
}

// Frees resources if buffer is no longer used
func (cb *columnarBufferArrowIPCStreamingDefault[T]) Release() {
	// cleanup builders
	for _, b := range cb.builders {
		b.Release()
	}

	// Release the stored Arrow Record if it exists
	if cb.arrowRecord != nil {
		cb.arrowRecord.Release()
		cb.arrowRecord = nil
	}
}
