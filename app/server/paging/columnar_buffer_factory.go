package paging

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/memory"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"
)

type columnarBufferFactoryImpl[T utils.Acceptor] struct {
	arrowAllocator memory.Allocator
	logger         *zap.Logger
	format         api_service_protos.TReadSplitsRequest_EFormat
	schema         *arrow.Schema
	ydbTypes       []*Ydb.Type
}

func (cbf *columnarBufferFactoryImpl[T]) MakeBuffer() (ColumnarBuffer[T], error) {
	switch cbf.format {
	case api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING:
		builders, err := utils.YdbTypesToArrowBuilders(cbf.ydbTypes, cbf.arrowAllocator)
		if err != nil {
			return nil, fmt.Errorf("convert Select.What to arrow.Schema: %w", err)
		}

		if len(cbf.ydbTypes) == 0 {
			return &columnarBufferArrowIPCStreamingEmptyColumns[T]{
				arrowAllocator: cbf.arrowAllocator,
				schema:         cbf.schema,
				rowsAdded:      0,
			}, nil
		}

		return &columnarBufferArrowIPCStreamingDefault[T]{
			arrowAllocator: cbf.arrowAllocator,
			builders:       builders,
			schema:         cbf.schema,
			logger:         cbf.logger,
		}, nil
	default:
		return nil, fmt.Errorf("unknown format: %v", cbf.format)
	}
}

func NewColumnarBufferFactory[T utils.Acceptor](
	logger *zap.Logger,
	arrowAllocator memory.Allocator,
	format api_service_protos.TReadSplitsRequest_EFormat,
	selectWhat *api_service_protos.TSelect_TWhat,
) (ColumnarBufferFactory[T], error) {
	ydbTypes, err := utils.SelectWhatToYDBTypes(selectWhat)
	if err != nil {
		return nil, fmt.Errorf("convert Select.What to Ydb types: %w", err)
	}

	schema, err := utils.SelectWhatToArrowSchema(selectWhat)
	if err != nil {
		return nil, fmt.Errorf("convert Select.What to Arrow schema: %w", err)
	}

	cbf := &columnarBufferFactoryImpl[T]{
		logger:         logger,
		arrowAllocator: arrowAllocator,
		format:         format,
		schema:         schema,
		ydbTypes:       ydbTypes,
	}

	return cbf, nil
}
