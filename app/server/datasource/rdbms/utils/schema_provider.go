package utils //nolint:revive

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/common"
)

type defaultSchemaProvider struct {
	typeMapper      datasource.TypeMapper
	getArgsAndQuery func(request *api_service_protos.TDescribeTableRequest) (string, *QueryArgs)
}

var _ SchemaProvider = (*defaultSchemaProvider)(nil)

func (f *defaultSchemaProvider) GetSchema(
	ctx context.Context,
	logger *zap.Logger,
	connMgr ConnectionManager,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TSchema, error) {
	params := &ConnectionParams{
		Ctx:                ctx,
		Logger:             logger,
		DataSourceInstance: request.DataSourceInstance,
		TableName:          request.Table,
		QueryPhase:         QueryPhaseDescribeTable,
	}

	cs, err := connMgr.Make(params)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	defer connMgr.Release(ctx, logger, cs)

	// We asked for a single connection
	conn := cs[0]

	query, args := f.getArgsAndQuery(request)

	queryParams := &QueryParams{
		Ctx:       ctx,
		Logger:    logger,
		QueryText: query,
		QueryArgs: args,
	}

	queryResult, err := conn.Query(queryParams)
	if err != nil {
		return nil, fmt.Errorf("query builder error: %w", err)
	}

	defer func() { common.LogCloserError(logger, queryResult, "close query result") }()

	sb := NewSchemaBuilder(f.typeMapper, request.TypeMappingSettings)

	var (
		columnName *string
		typeName   *string
		precision  *uint64
		scale      *int64
	)

	rows := queryResult.Rows

	for rows.Next() {
		if err = rows.Scan(&columnName, &typeName, &precision, &scale); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}

		cd := &datasource.ColumnDescription{
			Name: *columnName,
			Type: *typeName,
		}

		if precision != nil {
			cd.Precision = new(uint8)
			*cd.Precision = uint8(*precision)
		}

		if scale != nil {
			cd.Scale = new(int8)
			*cd.Scale = int8(*scale)
		}

		if err = sb.AddColumn(cd); err != nil {
			return nil, fmt.Errorf("add column `%s` to schema builder: %w", cd.Name, err)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	schema, err := sb.Build(logger)
	if err != nil {
		return nil, fmt.Errorf("build schema for table '%s': %w", request.GetTable(), err)
	}

	return schema, nil
}

func NewDefaultSchemaProvider(
	typeMapper datasource.TypeMapper,
	getArgsAndQueryFunc func(request *api_service_protos.TDescribeTableRequest) (string, *QueryArgs),
) SchemaProvider {
	return &defaultSchemaProvider{
		typeMapper:      typeMapper,
		getArgsAndQuery: getArgsAndQueryFunc,
	}
}
