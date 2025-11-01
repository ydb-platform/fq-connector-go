package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

var ydbConfig = &config.TYdbConfig{
	Mode:                  config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE,
	OpenConnectionTimeout: "5s",
	PingConnectionTimeout: "5s",
}

func getColumnShardIDs(ctx context.Context, driver *ydb.Driver, prefix string) ([]uint64, error) {
	var shardIDs []uint64

	err := driver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		queryText := fmt.Sprintf("SELECT DISTINCT(TabletId) FROM `%s/.sys/primary_index_stats`", prefix)

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

			var tabletId uint64

			for {
				r, errInner := resultSet.NextRow(ctx)
				if errInner != nil {
					if errors.Is(errInner, io.EOF) {
						break
					}

					return fmt.Errorf("next result set: %w", errInner)
				}

				if errInner := r.Scan(&tabletId); errInner != nil {
					return fmt.Errorf("row scan: %w", errInner)
				}

				shardIDs = append(shardIDs, tabletId)
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	return shardIDs, nil
}
