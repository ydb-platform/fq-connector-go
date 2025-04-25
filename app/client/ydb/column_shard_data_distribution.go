package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"gonum.org/v1/gonum/stat"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"github.com/ydb-platform/fq-connector-go/app/client/utils"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
)

//nolint:gocyclo
func columnShardsDataDistribution(cmd *cobra.Command, _ []string) error {
	preset, err := utils.MakePreset(cmd)
	if err != nil {
		return fmt.Errorf("make preset: %w", err)
	}

	defer preset.Close()

	logger := preset.Logger
	ctx := context.Background()

	connManager := ydb.NewConnectionManager(ydbConfig, rdbms_utils.ConnectionManagerBase{})
	cs, err := connManager.Make(&rdbms_utils.ConnectionParams{
		Ctx:                ctx,
		Logger:             preset.Logger,
		DataSourceInstance: preset.Cfg.DataSourceInstance,
		QueryPhase:         rdbms_utils.QueryPhaseReadSplits,
	})

	if err != nil {
		return fmt.Errorf("make connection: %v", err)
	}

	defer connManager.Release(ctx, preset.Logger, cs)

	prefix := path.Join(cs[0].DataSourceInstance().Database, preset.TableName)
	driver := cs[0].(ydb.Connection).Driver()

	shardIDs, err := getColumnShardIDs(ctx, driver, prefix)
	if err != nil {
		return fmt.Errorf("get column shard ids: %w", err)
	}

	type rowsCountResult struct {
		shardId   uint64
		totalRows uint64
		err       error
	}

	resultChan := make(chan rowsCountResult, len(shardIDs))

	// Get the number of rows in each shard
	for _, shardId := range shardIDs {
		go func(shardId uint64) {
			err := driver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
				queryText := fmt.Sprintf("SELECT COUNT(*) FROM `%s` WITH (ShardId=\"%d\")", prefix, shardId)

				result, errInner := s.Query(ctx, queryText)
				if errInner != nil {
					return fmt.Errorf("query: %w", errInner)
				}

				for {
					resultSet, errInner := result.NextResultSet(ctx)
					if errInner != nil {
						if errors.Is(errInner, io.EOF) {
							break
						}

						return fmt.Errorf("next result set: %w", errInner)
					}

					for {
						r, errInner := resultSet.NextRow(ctx)
						if errInner != nil {
							if errors.Is(errInner, io.EOF) {
								break
							}

							return fmt.Errorf("next row: %w", errInner)
						}

						var records uint64

						if errInner := r.Scan(&records); errInner != nil {
							return fmt.Errorf("row scan: %w", errInner)
						}

						logger.Info("rows count result", zap.Uint64("shard_id", shardId), zap.Uint64("records", records))

						resultChan <- rowsCountResult{shardId: shardId, totalRows: records}
					}
				}

				return nil
			})

			if err != nil {
				resultChan <- rowsCountResult{shardId: shardId, err: err}
			}
		}(shardId)
	}

	// how many rows are there in each shard
	var totalRowsPerShard []float64

	for range shardIDs {
		result := <-resultChan

		if result.err != nil {
			return fmt.Errorf("query: %v", result.err)
		}

		totalRowsPerShard = append(totalRowsPerShard, float64(result.totalRows))
	}

	mean := stat.Mean(totalRowsPerShard, nil)
	if mean == 0 {
		return fmt.Errorf("coefficient of variation is undefined for mean = 0")
	}

	stdDev := stat.StdDev(totalRowsPerShard, nil)
	cv := (stdDev / mean) * 100 // CV as a percentage

	fmt.Printf("Mean: %.2f\n", mean)
	fmt.Printf("Standard Deviation: %.2f\n", stdDev)
	fmt.Printf("Coefficient of Variation: %.2f%%\n", cv)

	return nil
}
