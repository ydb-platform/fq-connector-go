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
				MaxConnections:     1, // single connection is enough to get metadata
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
	var cs []rdbms_utils.Connection

	err := ds.retrierSet.MakeConnection.Run(ctx, logger,
		func() error {
			var makeConnErr error

			params := &rdbms_utils.ConnectionParams{
				Ctx:                ctx,
				Logger:             logger,
				DataSourceInstance: slct.GetDataSourceInstance(),
				TableName:          slct.GetFrom().GetTable(),
				MaxConnections:     1, // single connection is enough to get metadata
			}

			cs, makeConnErr = ds.connectionManager.Make(params)
			if makeConnErr != nil {
				return fmt.Errorf("make connection: %w", makeConnErr)
			}

			return nil
		},
	)

	if err != nil {
		return fmt.Errorf("retry: %w", err)
	}

	defer ds.connectionManager.Release(ctx, logger, cs)

	if err := ds.splitProvider.ListSplits(ctx, logger, cs[0], request, slct, resultChan); err != nil {
		return fmt.Errorf("list splits: %w", err)
	}

	return nil
}

func (ds *dataSourceImpl) ReadSplit(
	ctx context.Context,
	logger *zap.Logger,
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

	sinkParams := make([]*paging.SinkParams, len(cs))
	for i, conn := range cs {
		sinkParams[i] = &paging.SinkParams{
			Logger: conn.Logger(),
		}
	}

	// Prepare sinks that will accept the data from the connections.
	sinks, err := sinkFactory.MakeSinks(sinkParams)
	if err != nil {
		return fmt.Errorf("make sinks: %w", err)
	}

	// Read data from every connection in a distinct goroutine.
	// TODO: check if it's OK to override context
	group, ctx := errgroup.WithContext(ctx)

	for i, conn := range cs {
		conn := conn
		sink := sinks[i]

		group.Go(func() error {
			return ds.doReadSplitSingleConn(ctx, logger, request, split, sink, conn)
		})
	}

	return group.Wait()
}

func (ds *dataSourceImpl) doReadSplitSingleConn(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
	conn rdbms_utils.Connection,
) error {
	_, tableName := conn.From()

	readSplitsQuery, err := rdbms_utils.MakeSelectQuery(
		ctx,
		logger,
		ds.sqlFormatter,
		split,
		request.Filtering,
		tableName,
	)

	if err != nil {
		return fmt.Errorf("make read split query: %w", err)
	}

	var rows rdbms_utils.Rows

	err = ds.retrierSet.Query.Run(
		ctx,
		logger,
		func() error {
			var queryErr error

			if rows, queryErr = conn.Query(&readSplitsQuery.QueryParams); queryErr != nil {
				return fmt.Errorf("query '%s' error: %w", readSplitsQuery.QueryText, queryErr)
			}

			return nil
		},
	)

	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	defer func() { common.LogCloserError(logger, rows, "close rows") }()

	transformer, err := rows.MakeTransformer(readSplitsQuery.YdbTypes, ds.converterCollection)
	if err != nil {
		return fmt.Errorf("make transformer: %w", err)
	}

	for cont := true; cont; cont = rows.NextResultSet() {
		for rows.Next() {
			if err := rows.Scan(transformer.GetAcceptors()...); err != nil {
				return fmt.Errorf("rows scan: %w", err)
			}

			if err := sink.AddRow(transformer); err != nil {
				return fmt.Errorf("add row to paging writer: %w", err)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	}

	// Notify parent that there will be no more data from this connection.
	sink.Finish()

	return nil
}

func NewDataSource(
	logger *zap.Logger,
	preset *Preset,
	converterCollection conversion.Collection,
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
	}
}
