package server

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/s3"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/streaming"
)

type DataSourceCollection struct {
	rdbms              datasource.Factory[any]
	memoryAllocator    memory.Allocator
	readLimiterFactory *paging.ReadLimiterFactory
	cfg                *config.TServerConfig
}

func (dsc *DataSourceCollection) DescribeTable(
	ctx context.Context, logger *zap.Logger, request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	kind := request.GetDataSourceInstance().GetKind()

	switch kind {
	case api_common.EDataSourceKind_CLICKHOUSE, api_common.EDataSourceKind_POSTGRESQL, api_common.EDataSourceKind_YDB:
		ds, err := dsc.rdbms.Make(logger, kind)
		if err != nil {
			return nil, err
		}

		return ds.DescribeTable(ctx, logger, request)
	case api_common.EDataSourceKind_S3:
		ds := s3.NewDataSource()

		return ds.DescribeTable(ctx, logger, request)
	default:
		return nil, fmt.Errorf("unsupported data source type '%v': %w", kind, common.ErrDataSourceNotSupported)
	}
}

func (dsc *DataSourceCollection) DoReadSplit(
	logger *zap.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
) error {
	switch kind := split.GetSelect().GetDataSourceInstance().GetKind(); kind {
	case api_common.EDataSourceKind_CLICKHOUSE, api_common.EDataSourceKind_POSTGRESQL, api_common.EDataSourceKind_YDB:
		ds, err := dsc.rdbms.Make(logger, kind)
		if err != nil {
			return err
		}

		return readSplit[any](logger, stream, request.GetFormat(), split, ds, dsc.memoryAllocator, dsc.readLimiterFactory, dsc.cfg)
	case api_common.EDataSourceKind_S3:
		ds := s3.NewDataSource()

		return readSplit[string](logger, stream, request.GetFormat(), split, ds, dsc.memoryAllocator, dsc.readLimiterFactory, dsc.cfg)
	default:
		return fmt.Errorf("unsupported data source type '%v': %w", kind, common.ErrDataSourceNotSupported)
	}
}

func readSplit[T paging.Acceptor](
	logger *zap.Logger,
	stream api_service.Connector_ReadSplitsServer,
	format api_service_protos.TReadSplitsRequest_EFormat,
	split *api_service_protos.TSplit,
	dataSource datasource.DataSource[T],
	memoryAllocator memory.Allocator,
	readLimiterFactory *paging.ReadLimiterFactory,
	cfg *config.TServerConfig,
) error {
	logger.Debug("split reading started", common.SelectToFields(split.Select)...)

	columnarBufferFactory, err := paging.NewColumnarBufferFactory[T](
		logger,
		memoryAllocator,
		format,
		split.Select.What)
	if err != nil {
		return fmt.Errorf("new columnar buffer factory: %w", err)
	}

	trafficTracker := paging.NewTrafficTracker[T](cfg.Paging)

	sink, err := paging.NewSink(
		stream.Context(),
		logger,
		trafficTracker,
		columnarBufferFactory,
		readLimiterFactory.MakeReadLimiter(logger),
		int(cfg.Paging.PrefetchQueueCapacity),
	)
	if err != nil {
		return fmt.Errorf("new sink: %w", err)
	}

	streamer := streaming.NewStreamer(
		logger,
		stream,
		split,
		sink,
		dataSource,
	)

	if err := streamer.Run(); err != nil {
		return fmt.Errorf("run paging streamer: %w", err)
	}

	readStats := trafficTracker.DumpStats(true)

	logger.Debug(
		"split reading finished",
		zap.Uint64("total_bytes", readStats.GetBytes()),
		zap.Uint64("total_rows", readStats.GetRows()),
	)

	return nil
}

func NewDataSourceCollection(
	queryLoggerFactory common.QueryLoggerFactory,
	memoryAllocator memory.Allocator,
	readLimiterFactory *paging.ReadLimiterFactory,
	cfg *config.TServerConfig,
) *DataSourceCollection {
	return &DataSourceCollection{
		rdbms:              rdbms.NewDataSourceFactory(queryLoggerFactory),
		memoryAllocator:    memoryAllocator,
		readLimiterFactory: readLimiterFactory,
		cfg:                cfg,
	}
}
