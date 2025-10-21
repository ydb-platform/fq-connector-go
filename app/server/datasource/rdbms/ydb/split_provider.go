package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_options "github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb/table_metadata_cache"
)

var _ rdbms_utils.SplitProvider = (*SplitProvider)(nil)

type SplitProvider struct {
	cfg                *config.TYdbConfig_TSplitting
	tableMetadataCache table_metadata_cache.Cache
}

//nolint:gocyclo
func (sp SplitProvider) ListSplits(
	params *rdbms_utils.ListSplitsParams,
) error {
	resultChan, slct, ctx, logger := params.ResultChan, params.Select, params.Ctx, params.Logger

	storeType := table_metadata_cache.EStoreType_STORE_TYPE_UNSPECIFIED

	// Try to get cached value - this may help us to save a connection
	cachedValue, cachedValueExists := sp.tableMetadataCache.Get(
		logger,
		params.Select.DataSourceInstance,
		params.Select.GetFrom().GetTable(),
	)
	if cachedValueExists && cachedValue != nil {
		logger.Info("obtained table metadata from cache", zap.Stringer("store_type", cachedValue.StoreType))

		// If we have STORE_TYPE_ROW or STORE_TYPE_UNSPECIFIED, that's a row table.
		// On the row table the splitting is not implemented yet,
		// so there's no need to connect the database to get table metadata:
		// just return a single split.
		if cachedValue.StoreType == table_metadata_cache.EStoreType_STORE_TYPE_ROW ||
			cachedValue.StoreType == table_metadata_cache.EStoreType_STORE_TYPE_UNSPECIFIED {
			if err := sp.listSingleSplit(ctx, slct, resultChan); err != nil {
				return fmt.Errorf("list splits data shard: %w", err)
			}

			return nil
		}
	}

	// Connect YDB to get some table metadata
	var cs []rdbms_utils.Connection

	err := params.MakeConnectionRetrier.Run(ctx, logger,
		func() error {
			var makeConnErr error

			makeConnectionParams := &rdbms_utils.ConnectionParams{
				Ctx:                ctx,
				Logger:             logger,
				DataSourceInstance: slct.GetDataSourceInstance(),
				TableName:          slct.GetFrom().GetTable(),
				QueryPhase:         rdbms_utils.QueryPhaseListSplits,
			}

			cs, makeConnErr = params.ConnectionManager.Make(makeConnectionParams)
			if makeConnErr != nil {
				return fmt.Errorf("make connection: %w", makeConnErr)
			}

			return nil
		},
	)

	if err != nil {
		return fmt.Errorf("retry: %w", err)
	}

	defer params.ConnectionManager.Release(ctx, logger, cs)

	conn := cs[0]

	// If the cache was empty, we have to obtain metadata (via DescribeTable)
	if !cachedValueExists {
		storeType, err = sp.getTableStoreType(ctx, logger, conn)
		if err != nil {
			return fmt.Errorf("get table store type: %w", err)
		}
	}

	switch storeType {
	case table_metadata_cache.EStoreType_STORE_TYPE_COLUMN:
		logger.Info("column shard table discovered")

		if sp.cfg.EnabledOnColumnShards {
			if err = sp.listSplitsColumnShard(ctx, logger, conn, slct, resultChan); err != nil {
				return fmt.Errorf("list splits column shard: %w", err)
			}
		} else {
			logger.Warn(
				"splitting is disabled in config, fallback to default (single split per table)")

			if err = sp.listSingleSplit(ctx, slct, resultChan); err != nil {
				return fmt.Errorf("list single split: %w", err)
			}
		}
	case table_metadata_cache.EStoreType_STORE_TYPE_ROW:
		logger.Info("data shard table discovered")

		if err = sp.listSingleSplit(ctx, slct, resultChan); err != nil {
			return fmt.Errorf("list splits data shard: %w", err)
		}
	case table_metadata_cache.EStoreType_STORE_TYPE_UNSPECIFIED:
		// Observed with OLTP tables at: 24.3.11.13
		logger.Warn("table store type is unspecified, fallback to default (single split per table)")

		if err = sp.listSingleSplit(ctx, slct, resultChan); err != nil {
			return fmt.Errorf("list single split: %w", err)
		}
	default:
		return fmt.Errorf("unsupported table store type: %v", storeType)
	}

	return nil
}

func (SplitProvider) getTableStoreType(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
) (table_metadata_cache.EStoreType, error) {
	var (
		driver = conn.(Connection).Driver()
		prefix = path.Join(conn.DataSourceInstance().Database, conn.TableName())
		desc   table_options.Description
	)

	logger.Debug("obtaining table store type", zap.String("prefix", prefix))

	// otherwise make `DescribeTable` call and cache the result
	err := driver.Table().Do(
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
		return table_metadata_cache.EStoreType_STORE_TYPE_UNSPECIFIED, fmt.Errorf("get table description: %w", err)
	}

	result := table_metadata_cache.EStoreType(desc.StoreType)
	logger.Info("obtained table store type from GRPC call", zap.Any("store_type", result))

	return result, nil
}

func (sp SplitProvider) listSplitsColumnShard(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	tabletIDs, err := sp.GetColumnShardTabletIDs(ctx, logger, conn)
	if err != nil {
		return fmt.Errorf("enumerate shards: %w", err)
	}

	// There is a weird behavior of OLAP tables that have no data:
	// they do not return tablet ids at all, so in this case we have to return a single split
	if len(tabletIDs) == 0 {
		if err := sp.listSingleSplit(ctx, slct, resultChan); err != nil {
			return fmt.Errorf("list single split: %w", err)
		}

		return nil
	}

	// Otherwise emplace one tablet id per split
	for _, tabletId := range tabletIDs {
		description := &TSplitDescription{
			Payload: &TSplitDescription_ColumnShard{
				ColumnShard: &TSplitDescription_TColumnShard{
					TabletIds: []uint64{tabletId},
				},
			},
		}

		select {
		case resultChan <- makeSplit(slct, description):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (SplitProvider) doQueryTabletIDs(
	ctx context.Context,
	session query.Session,
	logger *zap.Logger,
	prefix string,
	attempt int,
) ([]uint64, error) {
	var tabletIDs []uint64

	queryText := fmt.Sprintf("SELECT DISTINCT(TabletId) FROM `%s/.sys/primary_index_stats`", prefix)

	logger.Debug("discovering column table tablet ids", zap.String("query", queryText), zap.Int("attempt", attempt))

	result, err := session.Query(ctx, queryText)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	for {
		resultSet, err := result.NextResultSet(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("next result set: %w", err)
		}

		var tabletId uint64

		for {
			r, err := resultSet.NextRow(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return nil, fmt.Errorf("next row: %w", err)
			}

			if err := r.Scan(&tabletId); err != nil {
				return nil, fmt.Errorf("row scan: %w", err)
			}

			tabletIDs = append(tabletIDs, tabletId)
		}
	}

	logger.Info("discovered column table tablet ids", zap.Int("total", len(tabletIDs)))

	return tabletIDs, nil
}

func (sp SplitProvider) GetColumnShardTabletIDs(
	parentCtx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
) ([]uint64, error) {
	driver := conn.(Connection).Driver()
	prefix := path.Join(conn.DataSourceInstance().Database, conn.TableName())

	var tabletIDs []uint64

	timeout, err := time.ParseDuration(sp.cfg.QueryTabletIdsTimeout)
	if err != nil {
		return nil, fmt.Errorf("parse query tablet ids timeout: %w", err)
	}

	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	attempts := 0

	err = driver.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		attempts++

		var queryErr error

		tabletIDs, queryErr = sp.doQueryTabletIDs(ctx, session, logger, prefix, attempts)
		if queryErr != nil {
			return fmt.Errorf("do query tablet ids: %w", queryErr)
		}

		return nil
	},
		query.WithIdempotent(),
	)

	if err != nil {
		return nil, fmt.Errorf("querying column table tablet ids: %w", err)
	}

	return tabletIDs, nil
}

// TODO: check request.MaxSplitCount (SLJ always wants a single split)
func (SplitProvider) listSingleSplit(
	ctx context.Context,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	// Data shard splitting is not supported yet
	splitDescription := &TSplitDescription{
		Payload: &TSplitDescription_DataShard{
			DataShard: &TSplitDescription_TDataShard{},
		},
	}

	select {
	case resultChan <- makeSplit(slct, splitDescription):
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func makeSplit(
	slct *api_service_protos.TSelect,
	description *TSplitDescription,
) *datasource.ListSplitResult {
	return &datasource.ListSplitResult{
		Slct:        slct,
		Description: description,
	}
}

func NewSplitProvider(cfg *config.TYdbConfig_TSplitting, tableMetadataCache table_metadata_cache.Cache) SplitProvider {
	return SplitProvider{
		cfg:                cfg,
		tableMetadataCache: tableMetadataCache,
	}
}
