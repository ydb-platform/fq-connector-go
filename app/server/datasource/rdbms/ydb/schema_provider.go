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
	connMgr rdbms_utils.ConnectionManager,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TSchema, error) {
	prefix := path.Join(request.DataSourceInstance.Database, request.Table)
	logger = logger.With(zap.String("prefix", prefix))

	logger.Debug("obtaining table metadata from YDB")

	// Try to get cached value - this helps us avoid creating a connection
	cachedValue, cachedValueExists := f.tableMetadataCache.Get(logger, request.DataSourceInstance, request.Table)
	if cachedValueExists && cachedValue != nil && cachedValue.Schema != nil {
		logger.Debug("obtained table metadata from cache")

		return cachedValue.Schema, nil
	}

	// Cache miss or empty schema - need to create connection and fetch from YDB
	params := &rdbms_utils.ConnectionParams{
		Ctx:                ctx,
		Logger:             logger,
		DataSourceInstance: request.DataSourceInstance,
		TableName:          request.Table,
		QueryPhase:         rdbms_utils.QueryPhaseDescribeTable,
	}

	cs, err := connMgr.Make(params)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	defer connMgr.Release(ctx, logger, cs)

	// We asked for a single connection
	conn := cs[0]

	var (
		driver = conn.(Connection).Driver()
		desc   options.Description
	)

	err = driver.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			var errInner error

			desc, errInner = s.DescribeTable(ctx, prefix)
			if errInner != nil {
				return fmt.Errorf("describe table '%v': %w", prefix, errInner)
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

	// preserve table metadata into cache to decrease the latency of DescribeTable and ListSplits methods
	value := &table_metadata_cache.TValue{
		Schema:    schema,
		StoreType: table_metadata_cache.EStoreType(desc.StoreType),
	}

	ok := f.tableMetadataCache.Put(logger, request.DataSourceInstance, request.Table, value)
	if !ok {
		logger.Warn("failed to cache table metadata")
	} else {
		logger.Debug("cached table metadata")
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
