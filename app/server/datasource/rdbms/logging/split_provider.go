package logging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ rdbms_utils.SplitProvider = (*splitProviderImpl)(nil)

type splitProviderImpl struct{}

func (s *splitProviderImpl) ListSplits(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	request *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
) (<-chan *datasource.ListSplitResult, error) {
	resultChan := make(chan *datasource.ListSplitResult, 64)

	// If client refused to split the table, return just one split containing the whole table.
	if request.MaxSplitCount == 1 {
		resultChan <- makeSingleSplit(slct)
		return resultChan, nil
	}

	// Otherwise connect to the database to obtain table metadata and
	// determine table partitions.
	go func() {
		defer close(resultChan)

		err := s.doListSplits(ctx, logger, conn, resultChan)
		if err != nil {
			resultChan <- &datasource.ListSplitResult{Error: err}
		}
	}()

	return resultChan, nil
}

func (s *splitProviderImpl) doListSplits(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	driver := conn.(ydb.Connection).Driver()
	databaseName, tableName := conn.From()
	prefix := path.Join(databaseName, tableName)

	logger.Debug("Obtaining table shard ids", zap.String("prefix", prefix))

	var tabletIds []uint64

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

			type rowData struct {
				TabletId uint64 `sql:"TabletId"`
			}

			var row rowData

			for {
				r, err := resultSet.NextRow(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return fmt.Errorf("next result set: %w", err)
				}

				if err := r.Scan(&row); err != nil {
					return fmt.Errorf("row scan: %w", err)
				}

				tabletIds = append(tabletIds, row.TabletId)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("querying table shard ids: %w", err)
	}

	fmt.Println("TABLET IDS:", tabletIds)

	return nil
}

func makeSingleSplit(slct *api_service_protos.TSelect) *datasource.ListSplitResult {
	return &datasource.ListSplitResult{
		Slct:        slct,
		Description: []byte{},
		Error:       nil,
	}
}

func NewSplitProvider() rdbms_utils.SplitProvider {
	return &splitProviderImpl{}
}
