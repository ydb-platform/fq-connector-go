package paging

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

type ColumnarBuffer[T Acceptor] interface {
	// addRow saves a row obtained from the datasource into the columnar buffer
	addRow(rowTransformer RowTransformer[T]) error
	// ToResponse returns all the accumulated data and clears buffer
	ToResponse() (*api_service_protos.TReadSplitsResponse, error)
	// Release frees resources if buffer is no longer used
	Release()
	// TotalRows return the number of rows accumulated
	TotalRows() int
}

type ColumnarBufferFactory[T Acceptor] interface {
	MakeBuffer() (ColumnarBuffer[T], error)
}

// ReadResult is an algebraic data type containing:
// 1. a buffer (e. g. page) packed with data
// 2. stats describing data that is kept in buffer
// 3. result of read operation (potentially with error)
// 4. flag marking this stream as completed
type ReadResult[T Acceptor] struct {
	ColumnarBuffer    ColumnarBuffer[T]
	Stats             *api_service_protos.TReadSplitsResponse_TStats
	Error             error
	IsTerminalMessage bool
}

// Sink is a destination for a data stream that is read out of an external data source connection.
type Sink[T Acceptor] interface {
	// AddRow saves the row obtained from a stream incoming from an external data source.
	AddRow(rowTransformer RowTransformer[T]) error

	// Finish reports the successful completion of data stream reading.
	Finish()
}
