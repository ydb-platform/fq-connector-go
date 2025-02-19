package ydb

import (
	"context"
	"fmt"
	"path"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
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
	databaseName, tableName := conn.From()

	var (
		driver = conn.(ydbConnection).getDriver()
		prefix = path.Join(databaseName, tableName)
		desc   options.Description
	)

	logger.Debug("obtaining table metadata", zap.String("prefix", prefix))

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
		return fmt.Errorf("get table description: %w", err)
	}

	fmt.Println(desc)

	return nil
}

func makeSingleSplit(slct *api_service_protos.TSelect) *datasource.ListSplitResult {
	return &datasource.ListSplitResult{
		Slct:        slct,
		Description: []byte{},
		Error:       nil,
	}
}
