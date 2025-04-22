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
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/nosql/mongodb"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/nosql/redis"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/s3"
	"github.com/ydb-platform/fq-connector-go/app/server/observation"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/streaming"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

type DataSourceCollection struct {
	rdbms               datasource.Factory[any]
	memoryAllocator     memory.Allocator
	readLimiterFactory  *paging.ReadLimiterFactory
	converterCollection conversion.Collection
	observationStorage  observation.Storage
	cfg                 *config.TServerConfig
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
	case api_common.EGenericDataSourceKind_MONGO_DB:
		mongoDbCfg := dsc.cfg.Datasources.Mongodb
		ds := mongodb.NewDataSource(
			&retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(mongoDbCfg.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(mongoDbCfg.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
			dsc.converterCollection,
			mongoDbCfg,
		)

		return ds.DescribeTable(ctx, logger, request)
	case api_common.EGenericDataSourceKind_REDIS:
		redisCfg := dsc.cfg.Datasources.Redis
		ds := redis.NewDataSource(
			&retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(redisCfg.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(redisCfg.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
			redisCfg,
			dsc.converterCollection,
		)

		return ds.DescribeTable(ctx, logger, request)

	default:
		return nil, fmt.Errorf("unsupported data source type '%v': %w", kind, common.ErrDataSourceNotSupported)
	}
}

func (dsc *DataSourceCollection) ListSplits(
	logger *zap.Logger,
	stream api_service.Connector_ListSplitsServer,
	request *api_service_protos.TListSplitsRequest,
) error {
	for _, slct := range request.GetSelects() {
		kind := slct.GetDataSourceInstance().GetKind()

		switch kind {
		case api_common.EGenericDataSourceKind_CLICKHOUSE, api_common.EGenericDataSourceKind_POSTGRESQL,
			api_common.EGenericDataSourceKind_YDB, api_common.EGenericDataSourceKind_MS_SQL_SERVER,
			api_common.EGenericDataSourceKind_MYSQL, api_common.EGenericDataSourceKind_GREENPLUM,
			api_common.EGenericDataSourceKind_ORACLE, api_common.EGenericDataSourceKind_LOGGING:
			ds, err := dsc.rdbms.Make(logger, kind)
			if err != nil {
				return fmt.Errorf("make data source: %w", err)
			}

			streamer := streaming.NewListSplitsStreamer(logger, stream, ds, request, slct)

			if err := streamer.Run(); err != nil {
				return fmt.Errorf("run streamer: %w", err)
			}
		case api_common.EGenericDataSourceKind_MONGO_DB:
			mongoDbCfg := dsc.cfg.Datasources.Mongodb
			ds := mongodb.NewDataSource(
				&retry.RetrierSet{
					MakeConnection: retry.NewRetrierFromConfig(mongoDbCfg.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
					Query:          retry.NewRetrierFromConfig(mongoDbCfg.ExponentialBackoff, retry.ErrorCheckerNoop),
				},
				dsc.converterCollection,
				mongoDbCfg,
			)

			streamer := streaming.NewListSplitsStreamer(logger, stream, ds, request, slct)

			if err := streamer.Run(); err != nil {
				return fmt.Errorf("run streamer: %w", err)
			}
		case api_common.EGenericDataSourceKind_REDIS:
			redisCfg := dsc.cfg.Datasources.Redis
			ds := redis.NewDataSource(
				&retry.RetrierSet{
					MakeConnection: retry.NewRetrierFromConfig(redisCfg.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
					Query:          retry.NewRetrierFromConfig(redisCfg.ExponentialBackoff, retry.ErrorCheckerNoop),
				},
				redisCfg,
				dsc.converterCollection,
			)

			streamer := streaming.NewListSplitsStreamer(logger, stream, ds, request, slct)

			if err := streamer.Run(); err != nil {
				return fmt.Errorf("run streamer: %w", err)
			}
		default:
			return fmt.Errorf("unsupported data source type '%v': %w", kind, common.ErrDataSourceNotSupported)
		}
	}

	return nil
}

func (dsc *DataSourceCollection) ReadSplit(
	logger *zap.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
) error {
	kind := split.GetSelect().GetDataSourceInstance().GetKind()

	// Register query for further analysis
	queryID, err := dsc.observationStorage.CreateIncomingQuery(kind)
	if err != nil {
		return fmt.Errorf("create query: %w", err)
	}

	switch kind {
	case api_common.EGenericDataSourceKind_CLICKHOUSE, api_common.EGenericDataSourceKind_POSTGRESQL,
		api_common.EGenericDataSourceKind_YDB, api_common.EGenericDataSourceKind_MS_SQL_SERVER,
		api_common.EGenericDataSourceKind_MYSQL, api_common.EGenericDataSourceKind_GREENPLUM,
		api_common.EGenericDataSourceKind_ORACLE, api_common.EGenericDataSourceKind_LOGGING:
		ds, err := dsc.rdbms.Make(logger, kind)
		if err != nil {
			return fmt.Errorf("make data source: %w", err)
		}

		return doReadSplit[any](logger, queryID, stream, request, split, ds, dsc.memoryAllocator, dsc.readLimiterFactory, dsc.observationStorage, dsc.cfg)
	case api_common.EGenericDataSourceKind_S3:
		ds := s3.NewDataSource()

		return doReadSplit[string](logger, queryID, stream, request, split, ds, dsc.memoryAllocator, dsc.readLimiterFactory, dsc.observationStorage, dsc.cfg)
	case api_common.EGenericDataSourceKind_MONGO_DB:
		mongoDbCfg := dsc.cfg.Datasources.Mongodb
		ds := mongodb.NewDataSource(
			&retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(mongoDbCfg.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(mongoDbCfg.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
			dsc.converterCollection,
			mongoDbCfg,
		)

		return doReadSplit(logger, queryID, stream, request, split, ds, dsc.memoryAllocator, dsc.readLimiterFactory, dsc.observationStorage, dsc.cfg)

	case api_common.EGenericDataSourceKind_REDIS:
		redisCfg := dsc.cfg.Datasources.Redis
		ds := redis.NewDataSource(
			&retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(redisCfg.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(redisCfg.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
			redisCfg,
			dsc.converterCollection,
		)

		return doReadSplit(logger, queryID, stream, request, split, ds, dsc.memoryAllocator, dsc.readLimiterFactory, dsc.observationStorage, dsc.cfg)

	default:
		return fmt.Errorf("unsupported data source type '%v': %w", kind, common.ErrDataSourceNotSupported)
	}
}

func doReadSplit[T paging.Acceptor](
	logger *zap.Logger,
	queryID observation.IncomingQueryID,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	dataSource datasource.DataSource[T],
	memoryAllocator memory.Allocator,
	readLimiterFactory *paging.ReadLimiterFactory,
	observationStorage observation.Storage,
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

	streamer := streaming.NewReadSplitsStreamer(
		logger,
		queryID,
		stream,
		request,
		split,
		sinkFactory,
		dataSource,
	)

	// Run streaming reading
	if err := streamer.Run(); err != nil {
		// Register query error
		cancelQueryErr := observationStorage.CancelIncomingQuery(queryID, err.Error(), sinkFactory.FinalStats())
		if cancelQueryErr != nil {
			logger.Error("observation storage cancel query", zap.Error(cancelQueryErr))
		}

		return fmt.Errorf("run paging streamer: %w", err)
	}

	readStats := sinkFactory.FinalStats()

	fields := common.SelectToFields(split.Select)
	fields = append(fields,
		zap.Uint64("total_bytes", readStats.GetBytes()),
		zap.Uint64("total_rows", readStats.GetRows()),
	)

	logger.Debug("split reading finished", fields...)

	// Register query success
	err = observationStorage.FinishIncomingQuery(queryID, readStats)
	if err != nil {
		return fmt.Errorf("observation storage finish query: %w", err)
	}

	return nil
}

func (dsc *DataSourceCollection) Close() error {
	return dsc.rdbms.Close()
}

func NewDataSourceCollection(
	queryLoggerFactory common.QueryLoggerFactory,
	memoryAllocator memory.Allocator,
	readLimiterFactory *paging.ReadLimiterFactory,
	converterCollection conversion.Collection,
	observationStorage observation.Storage,
	cfg *config.TServerConfig,
) (*DataSourceCollection, error) {
	rdbmsFactory, err := rdbms.NewDataSourceFactory(cfg.Datasources, queryLoggerFactory, converterCollection, observationStorage)
	if err != nil {
		return nil, fmt.Errorf("new rdbms data source factory: %w", err)
	}

	return &DataSourceCollection{
		rdbms:               rdbmsFactory,
		memoryAllocator:     memoryAllocator,
		readLimiterFactory:  readLimiterFactory,
		converterCollection: converterCollection,
		cfg:                 cfg,
	}, nil
}
