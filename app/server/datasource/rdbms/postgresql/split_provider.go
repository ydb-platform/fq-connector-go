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

	// If splitting is disabled, return single split for any table
	if !s.cfg.Enabled {
		if err := s.listSingleSplit(ctx, slct, resultChan); err != nil {
			return fmt.Errorf("list single split: %w", err)
		}

		return nil
	}

	// Connect database to get table metadata
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

	// Check the size of the table. There is no sense to split too small tables.
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

	if tablePhysicalSize < s.cfg.TablePhysicalSizeThresholdBytes {
		logger.Info(
			"table physical size is less than threshold: falling back to single split",
			zap.Uint64("table_physical_size", tablePhysicalSize),
			zap.Uint64("table_size_threshold_bytes", s.cfg.TablePhysicalSizeThresholdBytes),
		)

		if err := s.listSingleSplit(ctx, slct, resultChan); err != nil {
			return fmt.Errorf("list single split: %w", err)
		}

		return nil
	}

	logger.Info(
		"table physical size is greater than threshold: going to list splits",
		zap.Uint64("table_physical_size", tablePhysicalSize),
		zap.Uint64("table_size_threshold_bytes", s.cfg.TablePhysicalSizeThresholdBytes),
	)

	panic("not implemented yet")
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
		QueryText: "SELECT pg_table_size($1)",
		QueryArgs: args,
	}

	rows, err := conn.Query(queryParams)
	if err != nil {
		return 0, fmt.Errorf("conn query: %w", err)
	}
	defer rows.Close() // Add defer to ensure rows are closed

	var pgTableSize uint64

	// Add this line to position the cursor on the first row
	if !rows.Next() {
		return 0, fmt.Errorf("no rows returned from query")
	}

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
	return &splitProviderImpl{
		cfg: cfg,
	}
}
