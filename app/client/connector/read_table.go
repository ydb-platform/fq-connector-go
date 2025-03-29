package connector

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/client/utils"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

// These metadata fields are not used right now,
type requestMetadata struct {
	userID    string
	sessionID string
}

func readTable(cmd *cobra.Command, _ []string) error {
	dateTimeFormatStr, err := cmd.Flags().GetString(utils.DateTimeFormatFlag)
	if err != nil {
		return fmt.Errorf("get date-time-format flag: %v", err)
	}

	dateTimeFormat, exists := api_service_protos.EDateTimeFormat_value[dateTimeFormatStr]
	if !exists {
		return fmt.Errorf("unknown date-time-format: %s", dateTimeFormatStr)
	}

	userID, err := cmd.Flags().GetString(utils.UserIDFlag)
	if err != nil {
		return fmt.Errorf("get user flag: %v", err)
	}

	sessionID, err := cmd.Flags().GetString(utils.SessionIDFlag)
	if err != nil {
		return fmt.Errorf("get session flag: %v", err)
	}

	preset, err := utils.MakePreset(cmd)
	if err != nil {
		return fmt.Errorf("make preset: %w", err)
	}

	defer preset.Close()

	client, err := common.NewClientBufferingFromClientConfig(preset.Logger, preset.Cfg)
	if err != nil {
		return fmt.Errorf("new client buffering from client config: %w", err)
	}

	defer client.Close()

	md := requestMetadata{
		userID:    userID,
		sessionID: sessionID,
	}

	if err := doReadTable(
		preset.Logger,
		preset.Cfg,
		preset.TableName,
		api_service_protos.EDateTimeFormat(dateTimeFormat),
		md); err != nil {
		return fmt.Errorf("call server: %w", err)
	}

	return nil
}

func doReadTable(
	logger *zap.Logger,
	cfg *config.TClientConfig,
	tableName string,
	dateTimeFormat api_service_protos.EDateTimeFormat,
	metainfo requestMetadata) error {
	cl, err := common.NewClientBufferingFromClientConfig(logger, cfg)
	if err != nil {
		return fmt.Errorf("new client buffering from client config: %w", err)
	}

	defer cl.Close()

	var splits []*api_service_protos.TSplit

	switch cfg.DataSourceInstance.Kind {
	case api_common.EGenericDataSourceKind_CLICKHOUSE, api_common.EGenericDataSourceKind_POSTGRESQL,
		api_common.EGenericDataSourceKind_YDB, api_common.EGenericDataSourceKind_MS_SQL_SERVER,
		api_common.EGenericDataSourceKind_MYSQL, api_common.EGenericDataSourceKind_GREENPLUM,
		api_common.EGenericDataSourceKind_ORACLE, api_common.EGenericDataSourceKind_LOGGING,
		api_common.EGenericDataSourceKind_MONGO_DB, api_common.EGenericDataSourceKind_REDIS:
		typeMappingSettings := &api_service_protos.TTypeMappingSettings{
			DateTimeFormat: dateTimeFormat,
		}

		splits, err = describeTableAndListSplits(logger, cl, cfg.DataSourceInstance, typeMappingSettings, tableName, metainfo)
		if err != nil {
			return fmt.Errorf("prepare splits: %w", err)
		}

		logger.Info("got splits", zap.Int("total", len(splits)))
	default:
		return fmt.Errorf("unexpected data source kind %v", cfg.DataSourceInstance.Kind)
	}

	// ReadSplits
	if err := readSplits(logger, cl, splits, metainfo); err != nil {
		return fmt.Errorf("read splits: %w", err)
	}

	return nil
}

func describeTableAndListSplits(
	logger *zap.Logger,
	cl *common.ClientBuffering,
	dsi *api_common.TGenericDataSourceInstance,
	typeMappingSettings *api_service_protos.TTypeMappingSettings,
	tableName string,
	metainfo requestMetadata,
) ([]*api_service_protos.TSplit, error) {
	md := metadata.New(map[string]string{"user_id": metainfo.userID, "session_id": metainfo.sessionID})
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	// DescribeTable
	logger.Debug("describing table")

	describeTableResponse, err := cl.DescribeTable(ctx, dsi, typeMappingSettings, tableName)
	if err != nil {
		return nil, fmt.Errorf("describe table: %w", err)
	}

	if !common.IsSuccess(describeTableResponse.Error) {
		return nil, fmt.Errorf("describe table: %v", describeTableResponse.Error)
	}

	logger.Info("got table schema", zap.String("result", describeTableResponse.Schema.String()))

	// ListSplits - we want to SELECT *
	slct := &api_service_protos.TSelect{
		DataSourceInstance: dsi,
		What:               common.SchemaToSelectWhatItems(describeTableResponse.Schema, nil),
		From: &api_service_protos.TSelect_TFrom{
			Table: tableName,
		},
	}

	logger.Debug("listing splits", zap.String("select", slct.String()))

	listSplitsResponse, err := cl.ListSplits(ctx, slct)
	if err != nil {
		return nil, fmt.Errorf("list splits: %w", err)
	}

	logger.Info("got ListSplits responses", zap.Int("total_responses", len(listSplitsResponse)))

	return common.ListSplitsResponsesToSplits(listSplitsResponse), nil
}

func readSplits(
	logger *zap.Logger,
	cl *common.ClientBuffering,
	splits []*api_service_protos.TSplit,
	metainfo requestMetadata,
) error {
	md := metadata.New(map[string]string{"user_id": metainfo.userID, "session_id": metainfo.sessionID})
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	readSplitsResponses, err := cl.ReadSplits(ctx, splits)
	if err != nil {
		return fmt.Errorf("read splits: %w", err)
	}

	if err = common.ExtractErrorFromReadResponses(readSplitsResponses); err != nil {
		return fmt.Errorf("extract error from read responses: %w", err)
	}

	logger.Info("got ReadSplits responses", zap.Int("total_responses", len(readSplitsResponses)))

	records, err := common.ReadResponsesToArrowRecords(readSplitsResponses)
	if err != nil {
		return fmt.Errorf("read responses to arrow records: %w", err)
	}

	dumpReadResponses(logger, records)

	return nil
}

func dumpReadResponses(
	logger *zap.Logger,
	records []arrow.Record,
) {
	for i, record := range records {
		logger.Info(
			"dumping record",
			zap.Int("id", i),
			zap.Int("num_columns", int(record.NumCols())),
			zap.Int("num_rows", int(record.NumRows())),
		)

		for i, column := range record.Columns() {
			logger.Debug("column",
				zap.Int("id", i),
				zap.String("column_name", record.ColumnName(i)),
				zap.String("data", column.String()))
		}
	}
}
