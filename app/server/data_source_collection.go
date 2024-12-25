package server

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/s3"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/streaming"
	"github.com/ydb-platform/fq-connector-go/common"
)

type DataSourceCollection struct {
	rdbms              datasource.Factory[any]
	memoryAllocator    memory.Allocator
	readLimiterFactory *paging.ReadLimiterFactory
	cfg                *config.TServerConfig
}

func (dsc *DataSourceCollection) DescribeTable(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	kind := request.GetDataSourceInstance().GetKind()

	switch kind {
	case api_common.EGenericDataSourceKind_CLICKHOUSE, api_common.EGenericDataSourceKind_POSTGRESQL,
		api_common.EGenericDataSourceKind_YDB, api_common.EGenericDataSourceKind_MS_SQL_SERVER,
		api_common.EGenericDataSourceKind_MYSQL, api_common.EGenericDataSourceKind_GREENPLUM,
		api_common.EGenericDataSourceKind_ORACLE, api_common.EGenericDataSourceKind_LOGGING:
		ds, err := dsc.rdbms.Make(logger, kind)
		if err != nil {
			return nil, fmt.Errorf("make data source: %w", err)
		}

		return ds.DescribeTable(ctx, logger, request)
	case api_common.EGenericDataSourceKind_S3:
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
	case api_common.EGenericDataSourceKind_CLICKHOUSE, api_common.EGenericDataSourceKind_POSTGRESQL,
		api_common.EGenericDataSourceKind_YDB, api_common.EGenericDataSourceKind_MS_SQL_SERVER,
		api_common.EGenericDataSourceKind_MYSQL, api_common.EGenericDataSourceKind_GREENPLUM,
		api_common.EGenericDataSourceKind_ORACLE, api_common.EGenericDataSourceKind_LOGGING:
		ds, err := dsc.rdbms.Make(logger, kind)
		if err != nil {
			return fmt.Errorf("make data source: %w", err)
		}

		return readSplit[any](logger, stream, request, split, ds, dsc.memoryAllocator, dsc.readLimiterFactory, dsc.cfg)
	case api_common.EGenericDataSourceKind_S3:
		ds := s3.NewDataSource()

		return readSplit[string](logger, stream, request, split, ds, dsc.memoryAllocator, dsc.readLimiterFactory, dsc.cfg)
	default:
		return fmt.Errorf("unsupported data source type '%v': %w", kind, common.ErrDataSourceNotSupported)
	}
}

func readSplit[T paging.Acceptor](
	logger *zap.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
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
		request.Format,
		split.Select.What)
	if err != nil {
		return fmt.Errorf("new columnar buffer factory: %w", err)
	}

	sinkFactory := paging.NewSinkFactory[T](
		stream.Context(),
		logger,
		cfg.Paging,
		columnarBufferFactory,
		readLimiterFactory.MakeReadLimiter(logger),
	)

	streamer := streaming.NewStreamer(
		logger,
		stream,
		request,
		split,
		sinkFactory,
		dataSource,
	)

	if err := streamer.Run(); err != nil {
		return fmt.Errorf("run paging streamer: %w", err)
	}

	readStats := sinkFactory.FinalStats()

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
	converterCollection conversion.Collection,
	cfg *config.TServerConfig,
) (*DataSourceCollection, error) {
	rdbmsFactory, err := rdbms.NewDataSourceFactory(cfg.Datasources, queryLoggerFactory, converterCollection)
	if err != nil {
		return nil, fmt.Errorf("new rdbms data source factory: %w", err)
	}

	return &DataSourceCollection{
		rdbms:              rdbmsFactory,
		memoryAllocator:    memoryAllocator,
		readLimiterFactory: readLimiterFactory,
		cfg:                cfg,
	}, nil
}
