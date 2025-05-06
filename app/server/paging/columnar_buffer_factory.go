package paging

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

type columnarBufferFactoryImpl[T Acceptor] struct {
	arrowAllocator memory.Allocator
	logger         *zap.Logger
	format         api_service_protos.TReadSplitsRequest_EFormat
}

func (cbf *columnarBufferFactoryImpl[T]) MakeBuffer(selectWhat *api_service_protos.TSelect_TWhat) (ColumnarBuffer[T], error) {
	switch cbf.format {
	case api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING:
		schema, err := common.SelectWhatToArrowSchema(selectWhat)
		if err != nil {
			return nil, fmt.Errorf("convert Select.What to Arrow schema: %w", err)
		}

		builders, err := common.SelectWhatToArrowBuilders(selectWhat, cbf.arrowAllocator)
		if err != nil {
			return nil, fmt.Errorf("ydb types to arrow builders: %w", err)
		}

		if len(selectWhat.Items) == 0 {
			return &columnarBufferArrowIPCStreamingEmptyColumns[T]{
				arrowAllocator: cbf.arrowAllocator,
				schema:         schema,
				rowsAdded:      0,
			}, nil
		}

		return &columnarBufferArrowIPCStreamingDefault[T]{
			arrowAllocator: cbf.arrowAllocator,
			builders:       builders,
			schema:         schema,
			logger:         cbf.logger,
		}, nil
	default:
		return nil, fmt.Errorf("unknown format: %v", cbf.format)
	}
}

func NewColumnarBufferFactory[T Acceptor](
	logger *zap.Logger,
	arrowAllocator memory.Allocator,
	format api_service_protos.TReadSplitsRequest_EFormat,
) (ColumnarBufferFactory[T], error) {

	cbf := &columnarBufferFactoryImpl[T]{
		logger:         logger,
		arrowAllocator: arrowAllocator,
		format:         format,
	}

	return cbf, nil
}
