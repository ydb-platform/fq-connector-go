package logging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
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

	if err := s.doListSplits(ctx, logger, conn, slct, resultChan); err != nil {
		return fmt.Errorf("do list splits: %w", err)
	}

	return nil
}

func (splitProviderImpl) doListSplits(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	driver := conn.(ydb.Connection).Driver()
	databaseName, tableName := conn.From()
	prefix := path.Join(databaseName, tableName)

	logger.Debug("discovering column table shard ids", zap.String("prefix", prefix))

	var totalShards int

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
					TabletIds: []uint64{tabletId},
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
	})
	if err != nil {
		return fmt.Errorf("querying table shard ids: %w", err)
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
