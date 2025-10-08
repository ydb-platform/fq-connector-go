package utils

import (
	"context"
	"database/sql"
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
	conn Connection,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TSchema, error) {
	query, args := f.getArgsAndQuery(request)

	queryParams := &QueryParams{
		Ctx:       ctx,
		Logger:    logger,
		QueryText: query,
		QueryArgs: args,
	}

	rows, err := conn.Query(queryParams)
	if err != nil {
		return nil, fmt.Errorf("query builder error: %w", err)
	}

	defer func() { common.LogCloserError(logger, rows, "close rows") }()

	var (
		columnName string
		typeName   string
		precision  sql.NullInt64
		scale      sql.NullInt64
	)

	sb := NewSchemaBuilder(f.typeMapper, request.TypeMappingSettings)

	for rows.Next() {
		if err = rows.Scan(&columnName, &typeName, &precision, &scale); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}

		columnDescription := &datasource.ColumnDescription{
			Name: columnName,
			Type: typeName,
		}

		if precision.Valid {
			columnDescription.Precision = &precision.Int64
		}

		if scale.Valid {
			columnDescription.Scale = &scale.Int64
		}

		if err = sb.AddColumn(columnDescription); err != nil {
			return nil, fmt.Errorf("add column to schema builder: %w", err)
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
