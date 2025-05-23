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
)

var _ rdbms_utils.SplitProvider = (*SplitProvider)(nil)

type SplitProvider struct {
	cfg *config.TYdbConfig_TSplitting
}

func (s SplitProvider) ListSplits(
	params *rdbms_utils.ListSplitsParams,
) error {
	resultChan, slct, ctx, logger := params.ResultChan, params.Select, params.Ctx, params.Logger

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

	// Find out the type of a table
	storeType, err := s.getTableStoreType(ctx, logger, conn)
	if err != nil {
		return fmt.Errorf("get table store type: %w", err)
	}

	switch storeType {
	case table_options.StoreTypeColumn:
		logger.Info("column shard table discovered")

		if s.cfg.EnabledOnColumnShards {
			if err = s.listSplitsColumnShard(ctx, logger, conn, slct, resultChan); err != nil {
				return fmt.Errorf("list splits column shard: %w", err)
			}
		} else {
			logger.Warn(
				"splitting is disabled in config, fallback to default (single split per table)")

			if err = s.listSingleSplit(ctx, logger, conn, slct, resultChan); err != nil {
				return fmt.Errorf("list single split: %w", err)
			}
		}
	case table_options.StoreTypeRow:
		logger.Info("data shard table discovered")

		if err = s.listSingleSplit(ctx, logger, conn, slct, resultChan); err != nil {
			return fmt.Errorf("list splits column shard: %w", err)
		}
	case table_options.StoreTypeUnspecified:
		// Observed with OLTP tables at: 24.3.11.13
		logger.Warn("table store type is unspecified, fallback to default (single split per table)")

		if err = s.listSingleSplit(ctx, logger, conn, slct, resultChan); err != nil {
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
) (table_options.StoreType, error) {
	var (
		driver = conn.(Connection).Driver()
		prefix = path.Join(conn.DataSourceInstance().Database, conn.TableName())
		desc   table_options.Description
	)

	logger.Debug("obtaining table store type", zap.String("prefix", prefix))

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
		return table_options.StoreTypeUnspecified, fmt.Errorf("get table description: %w", err)
	}

	logger.Info("determined table store type", zap.Any("store_type", desc.StoreType))

	return desc.StoreType, nil
}

func (s SplitProvider) listSplitsColumnShard(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	tabletIDs, err := s.GetColumnShardTabletIDs(ctx, logger, conn)
	if err != nil {
		return fmt.Errorf("enumerate shards: %w", err)
	}

	// There is a weird behavior of OLAP tables that have no data:
	// they do not return tablet ids at all, so in this case we have to return a single split
	if len(tabletIDs) == 0 {
		if err := s.listSingleSplit(ctx, logger, conn, slct, resultChan); err != nil {
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

const getColumnShardsTabletIDsQueryTimeout = 10 * time.Second

func (SplitProvider) GetColumnShardTabletIDs(
	parentCtx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
) ([]uint64, error) {
	driver := conn.(Connection).Driver()
	prefix := path.Join(conn.DataSourceInstance().Database, conn.TableName())

	var tabletIDs []uint64

	ctx, cancel := context.WithTimeout(parentCtx, getColumnShardsTabletIDsQueryTimeout)
	defer cancel()

	err := driver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		queryText := fmt.Sprintf("SELECT DISTINCT(TabletId) FROM `%s/.sys/primary_index_stats`", prefix)

		logger.Debug("discovering column table tablet ids", zap.String("query", queryText))

		result, err := s.Query(ctx, queryText)
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}

		for {
			resultSet, err := result.NextResultSet(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return fmt.Errorf("next result set: %w", err)
			}

			var tabletId uint64

			for {
				r, err := resultSet.NextRow(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return fmt.Errorf("next row: %w", err)
				}

				if err := r.Scan(&tabletId); err != nil {
					return fmt.Errorf("row scan: %w", err)
				}

				tabletIDs = append(tabletIDs, tabletId)
			}
		}

		logger.Info("discovered column table tablet ids", zap.Int("total", len(tabletIDs)))

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
	_ *zap.Logger,
	_ rdbms_utils.Connection,
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

func NewSplitProvider(cfg *config.TYdbConfig_TSplitting) SplitProvider {
	return SplitProvider{
		cfg: cfg,
	}
}
