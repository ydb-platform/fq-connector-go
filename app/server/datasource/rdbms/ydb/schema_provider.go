package ydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

type schemaProvider struct {
	typeMapper   datasource.TypeMapper
	prefixGetter PrefixGetter
}

var _ rdbms_utils.SchemaProvider = (*schemaProvider)(nil)

func (f *schemaProvider) GetSchema(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TSchema, error) {
	ydbDriver := conn.(ydbConnection).getDriver()
	desc := options.Description{}

	prefix, err := f.prefixGetter.GetPrefix(ctx, logger, ydbDriver, request)
	if err != nil {
		return nil, fmt.Errorf("get prefix: %w", err)
	}

	logger.Debug("obtaining table metadata", zap.String("prefix", prefix))

	err = ydbDriver.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			var errInner error
			desc, errInner = s.DescribeTable(ctx, prefix)
			if errInner != nil {
				return fmt.Errorf("describe table: %w", errInner)
			}

			return nil
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return nil, fmt.Errorf("get table description: %w", err)
	}

	sb := rdbms_utils.NewSchemaBuilder(f.typeMapper, request.TypeMappingSettings)
	for _, column := range desc.Columns {
		if err = sb.AddColumn(column.Name, column.Type.String()); err != nil {
			return nil, fmt.Errorf("add column to schema builder: %w", err)
		}
	}

	schema, err := sb.Build(logger)

	if err != nil {
		return nil, fmt.Errorf("build schema: %w", err)
	}

	return schema, nil
}

func NewSchemaProvider(
	typeMapper datasource.TypeMapper,
	preffixGetter PrefixGetter,
) rdbms_utils.SchemaProvider {
	return &schemaProvider{
		typeMapper:   typeMapper,
		prefixGetter: preffixGetter,
	}
}
