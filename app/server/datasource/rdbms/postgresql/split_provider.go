package postgresql

import (
	"context"
	"fmt"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"

	"go.uber.org/zap"
)

var _ rdbms_utils.SplitProvider = (*splitProviderImpl)(nil)

type splitProviderImpl struct {
	cfg *config.TPostgreSQLConfig_TSplitting
}

func (s *splitProviderImpl) ListSplits(
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
				DataSourceInstance: slct.DataSourceInstance,
				TableName:          slct.From.Table,
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

	tablePhysicalSize, err := s.getTablePhysicalSize(
		ctx,
		logger,
		conn,
		slct.DataSourceInstance.GetPgOptions().Schema,
		slct.From.Table,
	)

	if err != nil {
		return fmt.Errorf("get table physical size: %w", err)
	}

	if tablePhysicalSize < s.cfg.TableSizeThresholdBytes {
		logger.Info(
			"table physical size is less than threshold: falling back to single split",
			zap.Uint64("table_physical_size", tablePhysicalSize),
			zap.Uint64("table_size_threshold_bytes", s.cfg.TableSizeThresholdBytes),
		)

		return s.listSingleSplit(ctx, slct, resultChan)
	}

	logger.Info("table physical size", zap.Uint64("table_physical_size", tablePhysicalSize))

}

func (s *splitProviderImpl) getTablePhysicalSize(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	schemaName, tableName string,
) (uint64, error) {

	fullyQualifiedTableName := schemaName + "." + tableName

	args := &rdbms_utils.QueryArgs{}
	args.AddUntyped(fullyQualifiedTableName)

	queryParams := &rdbms_utils.QueryParams{
		Ctx:       ctx,
		Logger:    logger,
		QueryText: "SELECT pg_table_size($p1)",
		QueryArgs: args,
	}

	rows, err := conn.Query(queryParams)
	if err != nil {
		return 0, fmt.Errorf("conn query: %w", err)
	}

	var pgTableSize uint64

	if err := rows.Scan(&pgTableSize); err != nil {
		return 0, fmt.Errorf("rows scan: %w", err)
	}

	return pgTableSize, nil
}

func (s splitProviderImpl) listSingleSplit(
	ctx context.Context,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	// Data shard splitting is not supported yet
	splitDescription := &TSplitDescription{
		Payload: &TSplitDescription_Single{
			Single: &TSplitDescription_TSingle{},
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

func NewSplitProvider(cfg *config.TPostgreSQLConfig_TSplitting) rdbms_utils.SplitProvider {
	return &splitProviderImpl{}
}
