package rdbms

import (
	"context"
	"fmt"

	"go.uber.org/zap"

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
	RetrierSet        *retry.RetrierSet
}

var _ datasource.DataSource[any] = (*dataSourceImpl)(nil)

type dataSourceImpl struct {
	typeMapper          datasource.TypeMapper
	sqlFormatter        rdbms_utils.SQLFormatter
	connectionManager   rdbms_utils.ConnectionManager
	schemaProvider      rdbms_utils.SchemaProvider
	retrierSet          *retry.RetrierSet
	converterCollection conversion.Collection
	logger              *zap.Logger
}

func (ds *dataSourceImpl) DescribeTable(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	var conn rdbms_utils.Connection

	err := ds.retrierSet.MakeConnection.Run(ctx, logger,
		func() error {
			var makeConnErr error

			params := &rdbms_utils.ConnectionParams{
				Ctx:                ctx,
				Logger:             logger,
				DataSourceInstance: request.DataSourceInstance,
				TableName:          request.Table,
			}

			conn, makeConnErr = ds.connectionManager.Make(params)
			if makeConnErr != nil {
				return fmt.Errorf("make connection: %w", makeConnErr)
			}

			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("retry: %w", err)
	}

	defer ds.connectionManager.Release(ctx, logger, conn)

	schema, err := ds.schemaProvider.GetSchema(ctx, logger, conn, request)
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}

	return &api_service_protos.TDescribeTableResponse{Schema: schema}, nil
}

func (ds *dataSourceImpl) doReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
) error {
	readSplitsQuery, err := rdbms_utils.MakeReadSplitsQuery(ctx, logger, ds.sqlFormatter, split.Select, request.Filtering)
	if err != nil {
		return fmt.Errorf("make read split query: %w", err)
	}

	var conn rdbms_utils.Connection

	err = ds.retrierSet.MakeConnection.Run(
		ctx,
		logger,
		func() error {
			var makeConnErr error

			params := &rdbms_utils.ConnectionParams{
				Ctx:                ctx,
				Logger:             logger,
				DataSourceInstance: split.Select.DataSourceInstance,
			}

			conn, makeConnErr = ds.connectionManager.Make(params)
			if makeConnErr != nil {
				return fmt.Errorf("make connection: %w", makeConnErr)
			}

			return nil
		},
	)

	if err != nil {
		return fmt.Errorf("make connection: %w", err)
	}

	defer ds.connectionManager.Release(ctx, logger, conn)

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

	ydbTypes, err := common.SelectWhatToYDBTypes(readSplitsQuery.What)
	if err != nil {
		return fmt.Errorf("convert Select.What to Ydb types: %w", err)
	}

	transformer, err := rows.MakeTransformer(ydbTypes, ds.converterCollection)
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

	return nil
}

func (ds *dataSourceImpl) ReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
) {
	err := ds.doReadSplit(ctx, logger, request, split, sink)
	if err != nil {
		sink.AddError(err)
	}

	sink.Finish()
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
		retrierSet:          preset.RetrierSet,
		converterCollection: converterCollection,
	}
}
