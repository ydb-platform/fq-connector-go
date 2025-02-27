package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_options "github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.SplitProvider = (*splitProviderImpl)(nil)

type splitProviderImpl struct{}

func (s *splitProviderImpl) ListSplits(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	request *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult) error {
	// If client refused to split the table, return just one split containing the whole table.
	if request.MaxSplitCount == 1 {
		select {
		case resultChan <- makeSplit(slct, nil):
		case <-ctx.Done():
		}

		return nil
	}

	// Find out the type of a table
	storeType, err := s.getTableStoreType(ctx, logger, conn)
	if err != nil {
		return fmt.Errorf("get table store type: %w", err)
	}

	switch storeType {
	case table_options.StoreTypeColumn:
		if err = s.listSplitsColumnShard(ctx, logger, conn, slct, resultChan); err != nil {
			return fmt.Errorf("list splits column shard: %w", err)
		}
	case table_options.StoreTypeRow:
		if err = s.listSplitsDataShard(ctx, logger, conn, slct, resultChan); err != nil {
			return fmt.Errorf("list splits data shard: %w", err)
		}
	default:
		return errors.New("unsupported table store type")
	}

	if err := s.listSplitsColumnShard(ctx, logger, conn, slct, resultChan); err != nil {
		return fmt.Errorf("do list splits: %w", err)
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
				// UNAVAILABLE error code causes endless retry in Go SDK,
				// so we have to make this error indistinguishable for SDK.
				if ydb.IsOperationError(errInner, Ydb.StatusIds_UNAVAILABLE) &&
					strings.Contains(errInner.Error(), "Schemeshard not available") {
					return errors.New("schemeshard not available")
				}

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
					Shard: &TSplitDescription_ColumnShard_{
						ColumnShard: &TSplitDescription_ColumnShard{
							ShardIds: []uint64{tabletId},
						},
					},
				}

				// TODO: rewrite it
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

func (splitProviderImpl) listSplitsDataShard(
	ctx context.Context,
	_ *zap.Logger,
	_ rdbms_utils.Connection,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	// Data shard splitting is not supported yet
	splitDescription := &TSplitDescription{
		Shard: &TSplitDescription_DataShard_{
			DataShard: &TSplitDescription_DataShard{},
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

func NewSplitProvider() rdbms_utils.SplitProvider {
	return &splitProviderImpl{}
}
