package rdbms

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

type Preset struct {
	SQLFormatter      rdbms_utils.SQLFormatter
	ConnectionManager rdbms_utils.ConnectionManager
	TypeMapper        datasource.TypeMapper
	SchemaProvider    rdbms_utils.SchemaProvider
}

var _ datasource.DataSource[any] = (*dataSourceImpl)(nil)

type dataSourceImpl struct {
	typeMapper        datasource.TypeMapper
	sqlFormatter      rdbms_utils.SQLFormatter
	connectionManager rdbms_utils.ConnectionManager
	schemaProvider    rdbms_utils.SchemaProvider
	logger            *zap.Logger
}

func (ds *dataSourceImpl) DescribeTable(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	conn, err := ds.connectionManager.Make(ctx, logger, request.DataSourceInstance)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	defer ds.connectionManager.Release(logger, conn)

	schema, err := ds.schemaProvider.GetSchema(ctx, logger, conn, request)
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}

	return &api_service_protos.TDescribeTableResponse{Schema: schema}, nil
}

func (ds *dataSourceImpl) doReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
) error {
	query, args, err := rdbms_utils.MakeReadSplitQuery(logger, ds.sqlFormatter, split.Select)
	if err != nil {
		return fmt.Errorf("make read split query: %w", err)
	}

	conn, err := ds.connectionManager.Make(ctx, logger, split.Select.DataSourceInstance)
	if err != nil {
		return fmt.Errorf("make connection: %w", err)
	}

	defer ds.connectionManager.Release(logger, conn)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query '%s' error: %w", query, err)
	}

	defer func() { common.LogCloserError(logger, rows, "close rows") }()

	ydbTypes, err := common.SelectWhatToYDBTypes(split.Select.What)
	if err != nil {
		return fmt.Errorf("convert Select.What to Ydb types: %w", err)
	}

	transformer, err := rows.MakeTransformer(ydbTypes)
	if err != nil {
		return fmt.Errorf("make transformer: %w", err)
	}

	// FIXME: use https://pkg.go.dev/database/sql#Rows.NextResultSet
	// Very important! Possible data loss.
	for rows.Next() {
		if err := rows.Scan(transformer.GetAcceptors()...); err != nil {
			return fmt.Errorf("rows scan error: %w", err)
		}

		if err := sink.AddRow(transformer); err != nil {
			return fmt.Errorf("add row to paging writer: %w", err)
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
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
) {
	err := ds.doReadSplit(ctx, logger, split, sink)
	if err != nil {
		sink.AddError(err)
	}

	sink.Finish()
}

func NewDataSource(
	logger *zap.Logger,
	preset *Preset,
) datasource.DataSource[any] {
	return &dataSourceImpl{
		logger:            logger,
		sqlFormatter:      preset.SQLFormatter,
		connectionManager: preset.ConnectionManager,
		typeMapper:        preset.TypeMapper,
		schemaProvider:    preset.SchemaProvider,
	}
}
