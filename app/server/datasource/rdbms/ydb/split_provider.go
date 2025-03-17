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

var _ rdbms_utils.SplitProvider = (*splitProviderImpl)(nil)

type splitProviderImpl struct {
	cfg *config.TYdbConfig_TSplitting
}

func (s *splitProviderImpl) ListSplits(
	params *rdbms_utils.ListSplitsParams,
) error {
	request, resultChan, slct, ctx, logger := params.Request, params.ResultChan, params.Select, params.Ctx, params.Logger

	// If client refused to split the table, return just one split containing the whole table.
	if request.MaxSplitCount == 1 {
		select {
		case resultChan <- makeSplit(slct, nil):
		case <-ctx.Done():
		}

		return nil
	}

	// Otherwise connect YDB to get some table metadata
	var cs []rdbms_utils.Connection

	err := params.MakeConnectionRetrier.Run(ctx, logger,
		func() error {
			var makeConnErr error

			makeConnectionParams := &rdbms_utils.ConnectionParams{
				Ctx:                ctx,
				Logger:             logger,
				DataSourceInstance: slct.GetDataSourceInstance(),
				TableName:          slct.GetFrom().GetTable(),
				MaxConnections:     1, // single connection is enough to get metadata
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

func (splitProviderImpl) getTableStoreType(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
) (table_options.StoreType, error) {
	databaseName, tableName := conn.From()

	var (
		driver = conn.(Connection).Driver()
		prefix = path.Join(databaseName, tableName)
		desc   table_options.Description
	)

	logger.Debug("obtaining table store type", zap.String("prefix", prefix))

	err := driver.Table().Do(
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
		return table_options.StoreTypeUnspecified, fmt.Errorf("get table description: %w", err)
	}

	return desc.StoreType, nil
}

func (splitProviderImpl) listSplitsColumnShard(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	driver := conn.(Connection).Driver()
	databaseName, tableName := conn.From()
	prefix := path.Join(databaseName, tableName)

	logger.Debug("discovering column table shard ids", zap.String("prefix", prefix))

	var totalShards int

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := driver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		queryText := fmt.Sprintf("SELECT DISTINCT(TabletId) FROM `%s/.sys/primary_index_stats`", prefix)

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

					return fmt.Errorf("next result set: %w", err)
				}

				if err := r.Scan(&tabletId); err != nil {
					return fmt.Errorf("row scan: %w", err)
				}

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

				totalShards++
			}
		}

		logger.Info("discovered column table shards", zap.Int("total", totalShards))

		return nil
	},
		query.WithIdempotent(),
	)

	if err != nil {
		return fmt.Errorf("querying table shard ids: %w", err)
	}

	return nil
}

func (splitProviderImpl) listSingleSplit(
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

func NewSplitProvider(cfg *config.TYdbConfig_TSplitting) rdbms_utils.SplitProvider {
	return &splitProviderImpl{
		cfg: cfg,
	}
}
