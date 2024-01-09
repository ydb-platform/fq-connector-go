package utils

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/common"
)

type DefaultSchemaProvider struct {
	typeMapper      datasource.TypeMapper
	getArgsAndQuery func(request *api_service_protos.TDescribeTableRequest) (string, []any)
}

var _ SchemaProvider = (*DefaultSchemaProvider)(nil)

func (f *DefaultSchemaProvider) GetSchema(
	ctx context.Context,
	logger *zap.Logger,
	conn Connection,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TSchema, error) {
	query, args := f.getArgsAndQuery(request)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query builder error: %w", err)
	}

	defer func() { common.LogCloserError(logger, rows, "close rows") }()

	var (
		columnName string
		typeName   string
	)

	sb := NewSchemaBuilder(f.typeMapper, request.TypeMappingSettings)

	for rows.Next() {
		if err = rows.Scan(&columnName, &typeName); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}

		if err = sb.AddColumn(columnName, typeName); err != nil {
			return nil, fmt.Errorf("add column to schema builder: %w", err)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	schema, err := sb.Build(logger)
	if err != nil {
		return nil, fmt.Errorf("build schema: %w", err)
	}

	return schema, nil
}

func NewDefaultSchemaProvider(
	typeMapper datasource.TypeMapper,
	getArgsAndQueryFunc func(request *api_service_protos.TDescribeTableRequest) (string, []any),
) SchemaProvider {
	return &DefaultSchemaProvider{
		typeMapper:      typeMapper,
		getArgsAndQuery: getArgsAndQueryFunc,
	}
}
