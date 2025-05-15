package rdbms

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/observation"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

type Preset struct {
	SQLFormatter      rdbms_utils.SQLFormatter
	ConnectionManager rdbms_utils.ConnectionManager
	TypeMapper        datasource.TypeMapper
	SchemaProvider    rdbms_utils.SchemaProvider
	SplitProvider     rdbms_utils.SplitProvider
	RetrierSet        *retry.RetrierSet
}

var _ datasource.DataSource[any] = (*dataSourceImpl)(nil)

type dataSourceImpl struct {
	typeMapper          datasource.TypeMapper
	sqlFormatter        rdbms_utils.SQLFormatter
	connectionManager   rdbms_utils.ConnectionManager
	schemaProvider      rdbms_utils.SchemaProvider
	splitProvider       rdbms_utils.SplitProvider
	retrierSet          *retry.RetrierSet
	converterCollection conversion.Collection
	observationStorage  observation.Storage
	logger              *zap.Logger
}

func (ds *dataSourceImpl) DescribeTable(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	var cs []rdbms_utils.Connection

	err := ds.retrierSet.MakeConnection.Run(ctx, logger,
		func() error {
			var makeConnErr error

			params := &rdbms_utils.ConnectionParams{
				Ctx:                ctx,
				Logger:             logger,
				DataSourceInstance: request.DataSourceInstance,
				TableName:          request.Table,
				QueryPhase:         rdbms_utils.QueryPhaseDescribeTable,
			}

			cs, makeConnErr = ds.connectionManager.Make(params)
			if makeConnErr != nil {
				return fmt.Errorf("make connection: %w", makeConnErr)
			}

			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("retry: %w", err)
	}

	defer ds.connectionManager.Release(ctx, logger, cs)

	// We asked for a single connection
	conn := cs[0]

	schema, err := ds.schemaProvider.GetSchema(ctx, logger, conn, request)
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}

	return &api_service_protos.TDescribeTableResponse{Schema: schema}, nil
}

func (ds *dataSourceImpl) ListSplits(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult) error {
	params := &rdbms_utils.ListSplitsParams{
		Ctx:                   ctx,
		Logger:                logger,
		MakeConnectionRetrier: ds.retrierSet.MakeConnection,
		ConnectionManager:     ds.connectionManager,
		Request:               request,
		Select:                slct,
		ResultChan:            resultChan,
	}

	if err := ds.splitProvider.ListSplits(params); err != nil {
		return fmt.Errorf("list splits: %w", err)
	}

	return nil
}

func (ds *dataSourceImpl) ReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	incomingQueryID observation.IncomingQueryID,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sinkFactory paging.SinkFactory[any],
) error {
	// Make connection(s) to the data source.
	var cs []rdbms_utils.Connection

	err := ds.retrierSet.MakeConnection.Run(
		ctx,
		logger,
		func() error {
			var makeConnErr error

			params := &rdbms_utils.ConnectionParams{
				Ctx:                ctx,
				Logger:             logger,
				DataSourceInstance: split.Select.DataSourceInstance,
				TableName:          split.Select.From.Table,
				Split:              split,
				QueryPhase:         rdbms_utils.QueryPhaseReadSplits,
			}

			cs, makeConnErr = ds.connectionManager.Make(params)
			if makeConnErr != nil {
				return fmt.Errorf("make connection: %w", makeConnErr)
			}

			return nil
		},
	)

	if err != nil {
		return fmt.Errorf("make connection: %w", err)
	}

	defer ds.connectionManager.Release(ctx, logger, cs)

	ydbTypes, err := common.SelectWhatToYDBTypes(split.Select.What)
	if err != nil {
		return fmt.Errorf("select what to YDB types: %w", err)
	}

	sinkParams := make([]*paging.SinkParams, len(cs))
	for i, conn := range cs {
		sinkParams[i] = &paging.SinkParams{
			Logger:   conn.Logger(),
			YdbTypes: ydbTypes,
		}
	}

	// Prepare sinks that will accept the data from the connections.
	sinks, err := sinkFactory.MakeSinks(sinkParams)
	if err != nil {
		return fmt.Errorf("make sinks: %w", err)
	}

	// Read data from every connection in a distinct goroutine.
	group := errgroup.Group{}

	for i, conn := range cs {
		conn := conn
		sink := sinks[i]

		group.Go(func() error {
			// generate SQL query
			query, err := rdbms_utils.MakeSelectQuery(
				ctx,
				logger,
				ds.sqlFormatter,
				split,
				request.Filtering,
				conn.TableName(),
			)
			if err != nil {
				return fmt.Errorf("make select query: %w", err)
			}

			// register outgoing request in storage
			outgoingQueryID, err := ds.observationStorage.CreateOutgoingQuery(
				logger, incomingQueryID, conn.DataSourceInstance(), query.QueryText, query.QueryArgs.Values())
			if err != nil {
				return fmt.Errorf("create outgoing query: %w", err)
			}

			// execute query
			rowsRead, err := ds.doReadSplitSingleConn(ctx, logger, query, sink, conn)
			if err != nil {
				// register error
				if cancelErr := ds.observationStorage.CancelOutgoingQuery(outgoingQueryID, err.Error()); cancelErr != nil {
					logger.Error("cancel outgoing query: %w", zap.Error(cancelErr))
				}

				return fmt.Errorf("do read split single conn: %w", err)
			}

			// register success
			if err := ds.observationStorage.FinishOutgoingQuery(outgoingQueryID, rowsRead); err != nil {
				logger.Error("finish outgoing query: %w", zap.Error(err))
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return fmt.Errorf("group wait: %w", err)
	}

	return nil
}

func (ds *dataSourceImpl) doReadSplitSingleConn(
	ctx context.Context,
	logger *zap.Logger,
	query *rdbms_utils.SelectQuery,
	sink paging.Sink[any],
	conn rdbms_utils.Connection,
) (int64, error) {
	var rows rdbms_utils.Rows

	err := ds.retrierSet.Query.Run(
		ctx,
		logger,
		func() error {
			var queryErr error

			if rows, queryErr = conn.Query(&query.QueryParams); queryErr != nil {
				return fmt.Errorf("query '%s' error: %w", query.QueryText, queryErr)
			}

			return nil
		},
	)

	if err != nil {
		return 0, fmt.Errorf("query: %w", err)
	}

	defer common.LogCloserError(logger, rows, "close rows")

	transformer, err := rows.MakeTransformer(query.YdbColumns, ds.converterCollection)
	if err != nil {
		return 0, fmt.Errorf("make transformer: %w", err)
	}

	rowsRead := int64(0)

	for cont := true; cont; cont = rows.NextResultSet() {
		for rows.Next() {
			rowsRead++

			if err := rows.Scan(transformer.GetAcceptors()...); err != nil {
				return 0, fmt.Errorf("rows scan: %w", err)
			}

			if err := sink.AddRow(transformer); err != nil {
				return 0, fmt.Errorf("add row to paging writer: %w", err)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows error: %w", err)
	}

	// Notify sink that there will be no more data from this connection.
	// Hours lost in attempts to move this call into defer: 2
	sink.Finish()

	return rowsRead, nil
}

func NewDataSource(
	logger *zap.Logger,
	preset *Preset,
	converterCollection conversion.Collection,
	observationStorage observation.Storage,
) datasource.DataSource[any] {
	return &dataSourceImpl{
		logger:              logger,
		sqlFormatter:        preset.SQLFormatter,
		connectionManager:   preset.ConnectionManager,
		typeMapper:          preset.TypeMapper,
		schemaProvider:      preset.SchemaProvider,
		splitProvider:       preset.SplitProvider,
		retrierSet:          preset.RetrierSet,
		converterCollection: converterCollection,
		observationStorage:  observationStorage,
	}
}
