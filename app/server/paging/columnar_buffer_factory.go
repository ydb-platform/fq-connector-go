package paging

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

type columnarBufferFactoryImpl[T Acceptor] struct {
	arrowAllocator  memory.Allocator
	logger          *zap.Logger
	format          api_service_protos.TReadSplitsRequest_EFormat
	schema          *arrow.Schema
	ydbTypes        []*Ydb.Type
	useArrowRecords bool // Whether to use Arrow records directly
}

func (cbf *columnarBufferFactoryImpl[T]) MakeBuffer() (ColumnarBuffer[T], error) {
	switch cbf.format {
	case api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING:
		// Special case for empty columns
		if len(cbf.ydbTypes) == 0 {
			return &columnarBufferArrowIPCStreamingEmptyColumns[T]{
				arrowAllocator: cbf.arrowAllocator,
				schema:         cbf.schema,
				rowsAdded:      0,
			}, nil
		}

		// Choose implementation based on whether we're using Arrow records directly
		if cbf.useArrowRecords {
			return &columnarBufferArrowIPCStreamingRecords[T]{
				arrowAllocator: cbf.arrowAllocator,
				schema:         cbf.schema,
				logger:         cbf.logger,
			}, nil
		} else {
			builders, err := common.YdbTypesToArrowBuilders(cbf.ydbTypes, cbf.arrowAllocator)
			if err != nil {
				return nil, fmt.Errorf("convert Select.What to arrow.Schema: %w", err)
			}

			return &columnarBufferArrowIPCStreamingRows[T]{
				arrowAllocator: cbf.arrowAllocator,
				builders:       builders,
				schema:         cbf.schema,
				logger:         cbf.logger,
			}, nil
		}
	default:
		return nil, fmt.Errorf("unknown format: %v", cbf.format)
	}
}

func NewColumnarBufferFactory[T Acceptor](
	logger *zap.Logger,
	arrowAllocator memory.Allocator,
	format api_service_protos.TReadSplitsRequest_EFormat,
	selectWhat *api_service_protos.TSelect_TWhat,
	useArrowRecords bool,
) (ColumnarBufferFactory[T], error) {
	ydbTypes, err := common.SelectWhatToYDBTypes(selectWhat)
	if err != nil {
		return nil, fmt.Errorf("convert Select.What to Ydb types: %w", err)
	}

	schema, err := common.SelectWhatToArrowSchema(selectWhat)
	if err != nil {
		return nil, fmt.Errorf("convert Select.What to Arrow schema: %w", err)
	}

	cbf := &columnarBufferFactoryImpl[T]{
		logger:          logger,
		arrowAllocator:  arrowAllocator,
		format:          format,
		schema:          schema,
		ydbTypes:        ydbTypes,
		useArrowRecords: useArrowRecords,
	}

	return cbf, nil
}
