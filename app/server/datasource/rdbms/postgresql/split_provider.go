package postgresql

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.SplitProvider = (*splitProviderImpl)(nil)

type splitProviderImpl struct {
	cfg *config.TPostgreSQLConfig_TSplitting
}

//nolint:gocyclo
func (s *splitProviderImpl) ListSplits(
	params *rdbms_utils.ListSplitsParams,
) error {
	resultChan, slct, ctx, logger := params.ResultChan, params.Select, params.Ctx, params.Logger
	schemaName, tableName := slct.DataSourceInstance.GetPgOptions().Schema, slct.From.Table

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
				TableName:          tableName,
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
			zap.Uint64("table_physical_size_threshold_bytes", s.cfg.TablePhysicalSizeThresholdBytes),
		)

		if err = s.listSingleSplit(ctx, slct, resultChan); err != nil {
			return fmt.Errorf("list single split: %w", err)
		}

		return nil
	}

	// Table is large enough for splitting. Let's check if it has primary keys.
	logger.Debug(
		"table physical size is greater than threshold: going to list splits",
		zap.Uint64("table_physical_size", tablePhysicalSize),
		zap.Uint64("table_physical_size_threshold_bytes", s.cfg.TablePhysicalSizeThresholdBytes),
	)

	primaryKeys, err := s.getTablePrimaryKeys(ctx, logger, conn, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("get table primary keys: %w", err)
	}

	var pk *primaryKey

	switch len(primaryKeys) {
	case 0:
		logger.Info("table has no primary key: falling back to single split")

		if err = s.listSingleSplit(ctx, slct, resultChan); err != nil {
			return fmt.Errorf("list single split: %w", err)
		}

		return nil
	case 1:
		pk = primaryKeys[0]
		logger.Info(
			"discovered primary key",
			zap.String("column_name", pk.columnName),
			zap.String("column_type", pk.columnType),
		)
	default:
		return fmt.Errorf("impossible situation: table has %d primary keys", len(primaryKeys))
	}

	// We've discovered primary key, add now lets extract the PostgreSQL's native histogram bounds
	// to use it for a "natural" splitting.
	histogramBounds, err := s.getHistogramBoundsForPrimaryKey(ctx, logger, conn, schemaName, tableName, pk)
	if err != nil {
		return fmt.Errorf("get histogram bounds for int primary key: %w", err)
	}

	if len(histogramBounds) == 0 {
		logger.Info("histogram bounds are empty: falling back to single split")

		if err := s.listSingleSplit(ctx, slct, resultChan); err != nil {
			return fmt.Errorf("list single split: %w", err)
		}
	}

	for _, item := range histogramBounds {
		splitDescription := &TSplitDescription{
			Payload: &TSplitDescription_HistogramBounds{
				HistogramBounds: item,
			},
		}

		select {
		case resultChan <- makeSplit(slct, splitDescription):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (splitProviderImpl) getTablePhysicalSize(
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
	defer rows.Close()

	var pgTableSize uint64

	if !rows.Next() {
		return 0, fmt.Errorf("no rows returned from query")
	}

	if err := rows.Scan(&pgTableSize); err != nil {
		return 0, fmt.Errorf("rows scan: %w", err)
	}

	return pgTableSize, nil
}

type primaryKey struct {
	columnName string
	columnType string
}

func (splitProviderImpl) getTablePrimaryKeys(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	schemaName, tableName string,
) ([]*primaryKey, error) {
	const queryText = `
SELECT
    kcu.column_name,
    c.data_type
FROM
    information_schema.table_constraints AS tc
JOIN
    information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
JOIN
    information_schema.columns AS c
    ON c.table_name = tc.table_name AND c.column_name = kcu.column_name AND c.table_schema = tc.table_schema
WHERE
    tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = $1
    AND tc.table_name = $2;
`

	args := &rdbms_utils.QueryArgs{}
	args.AddUntyped(schemaName)
	args.AddUntyped(tableName)

	queryParams := &rdbms_utils.QueryParams{
		Ctx:       ctx,
		Logger:    logger,
		QueryText: queryText,
		QueryArgs: args,
	}

	rows, err := conn.Query(queryParams)
	if err != nil {
		return nil, fmt.Errorf("conn query: %w", err)
	}
	defer rows.Close()

	var (
		columnName string
		columnType string
		results    []*primaryKey
	)

	for cont := true; cont; cont = rows.NextResultSet() {
		for rows.Next() {
			if err := rows.Scan(&columnName, &columnType); err != nil {
				return nil, fmt.Errorf("rows scan: %w", err)
			}

			results = append(results, &primaryKey{columnName: columnName, columnType: columnType})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}

	return results, nil
}

func (splitProviderImpl) getHistogramBoundsForPrimaryKey(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	schemaName, tableName string,
	pk *primaryKey,
) ([]*TSplitDescription_THistogramBounds, error) {
	// Determine if we should use int32, int64, or decimal based on column type
	switch pk.columnType {
	case "integer", "int", "int4", "serial":
		return getHistogramBoundsForPrimaryKeyGeneric[int32](ctx, logger, conn, schemaName, tableName, pk)
	case "bigint", "int8", "bigserial":
		return getHistogramBoundsForPrimaryKeyGeneric[int64](ctx, logger, conn, schemaName, tableName, pk)
	case "numeric", "decimal":
		return getHistogramBoundsForPrimaryKeyGeneric[string](ctx, logger, conn, schemaName, tableName, pk)
	default:
		return nil, fmt.Errorf("unsupported column type for histogram bounds: %s", pk.columnType)
	}
}

func getHistogramBoundsForPrimaryKeyGeneric[T int32 | int64 | string](
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	schemaName, tableName string,
	pk *primaryKey,
) ([]*TSplitDescription_THistogramBounds, error) {
	const queryText = `
SELECT
    histogram_bounds
FROM
    pg_stats
WHERE
    schemaname = $1
    AND tablename = $2
    AND attname = $3;
`

	args := &rdbms_utils.QueryArgs{}
	args.AddUntyped(schemaName)
	args.AddUntyped(tableName)
	args.AddUntyped(pk.columnName)

	queryParams := &rdbms_utils.QueryParams{
		Ctx:       ctx,
		Logger:    logger,
		QueryText: queryText,
		QueryArgs: args,
	}

	rows, err := conn.Query(queryParams)
	if err != nil {
		return nil, fmt.Errorf("conn query: %w", err)
	}

	defer rows.Close()

	var bounds []T

	if !rows.Next() {
		logger.Warn(
			"no histogram bounds found for primary key: run ANALYZE",
			zap.String("column_name", pk.columnName),
		)

		return nil, nil
	}

	if err := rows.Scan(&bounds); err != nil {
		return nil, fmt.Errorf("rows scan: %w", err)
	}

	logger.Debug("discovered histogram bounds", zap.String("column_name", pk.columnName), zap.Int("total_bounds", len(bounds)))

	// Now we need to transfer histogram bounds into splits
	result := make([]*TSplitDescription_THistogramBounds, 0, len(bounds)+1)

	// Add first open interval
	result = append(result, createHistogramBound(pk.columnName, nil, &bounds[0]))

	// Add intervals between bounds
	for i := 0; i < len(bounds)-1; i++ {
		result = append(result, createHistogramBound(pk.columnName, &bounds[i], &bounds[i+1]))
	}

	// Add last open interval
	result = append(result, createHistogramBound(pk.columnName, &bounds[len(bounds)-1], nil))

	return result, nil
}

func createHistogramBound[T int32 | int64 | string](columnName string, lower, upper *T) *TSplitDescription_THistogramBounds {
	var payload any

	// Use type assertion on the actual value to determine the type
	var zeroVal T
	switch any(zeroVal).(type) {
	case int32:
		var lowerVal, upperVal *wrapperspb.Int32Value
		if lower != nil {
			lowerVal = wrapperspb.Int32(any(*lower).(int32))
		}
		if upper != nil {
			upperVal = wrapperspb.Int32(any(*upper).(int32))
		}
		payload = &TSplitDescription_THistogramBounds_Int32Bounds{
			Int32Bounds: &TInt32Bounds{
				Lower: lowerVal,
				Upper: upperVal,
			},
		}
	case int64:
		var lowerVal, upperVal *wrapperspb.Int64Value
		if lower != nil {
			lowerVal = wrapperspb.Int64(any(*lower).(int64))
		}
		if upper != nil {
			upperVal = wrapperspb.Int64(any(*upper).(int64))
		}
		payload = &TSplitDescription_THistogramBounds_Int64Bounds{
			Int64Bounds: &TInt64Bounds{
				Lower: lowerVal,
				Upper: upperVal,
			},
		}
	case string:
		var lowerVal, upperVal *wrapperspb.StringValue
		if lower != nil {
			lowerVal = wrapperspb.String(any(*lower).(string))
		}
		if upper != nil {
			upperVal = wrapperspb.String(any(*upper).(string))
		}
		payload = &TSplitDescription_THistogramBounds_DecimalBounds{
			DecimalBounds: &TDecimalBounds{
				Lower: lowerVal,
				Upper: upperVal,
			},
		}
	}

	return &TSplitDescription_THistogramBounds{
		ColumnName: columnName,
		Payload:    payload.(isTSplitDescription_THistogramBounds_Payload),
	}
}

func (splitProviderImpl) listSingleSplit(
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
