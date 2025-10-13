package utils

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

	sb := NewSchemaBuilder(f.typeMapper, request.TypeMappingSettings)

	var (
		columnName *string
		typeName   *string
		precision  *uint64
		scale      *int64
	)

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
