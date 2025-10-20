package ydb

import (
	"context"
	"fmt"
	"path"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb/table_metadata_cache"
)

type schemaProvider struct {
	typeMapper         datasource.TypeMapper
	tableMetadataCache table_metadata_cache.Cache
}

var _ rdbms_utils.SchemaProvider = (*schemaProvider)(nil)

func (f *schemaProvider) GetSchema(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TSchema, error) {
	var (
		driver = conn.(Connection).Driver()
		prefix = path.Join(conn.DataSourceInstance().Database, conn.TableName())
		desc   options.Description
	)

	logger = logger.With(zap.String("prefix", prefix))

	logger.Debug("obtaining table metadata")

	err := driver.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			var errInner error

			desc, errInner = s.DescribeTable(ctx, prefix)
			if errInner != nil {
				return fmt.Errorf("describe table '%v': %w", prefix, errInner)
			}

			// preserve table store type into cache - it further can save ListSplits latency
			ok := f.tableMetadataCache.Put(request.DataSourceInstance, "tableName", desc.StoreType)
			if !ok {
				logger.Warn("failed to cache table store type", zap.Any("store_type", desc.StoreType))
			} else {
				logger.Info("cached table store type", zap.Any("store_type", desc.StoreType))
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
		desc := &datasource.ColumnDescription{
			Name:      column.Name,
			Type:      column.Type.String(),
			Precision: nil,
			Scale:     nil,
		}

		if err = sb.AddColumn(desc); err != nil {
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
	tableMetadataCache table_metadata_cache.Cache,
) rdbms_utils.SchemaProvider {
	return &schemaProvider{
		typeMapper:         typeMapper,
		tableMetadataCache: tableMetadataCache,
	}
}
