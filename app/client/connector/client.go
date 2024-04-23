package connector

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

func runClient(cmd *cobra.Command, _ []string) error {
	configPath, err := cmd.Flags().GetString(configFlag)
	if err != nil {
		return fmt.Errorf("get config flag: %v", err)
	}

	tableName, err := cmd.Flags().GetString(tableFlag)
	if err != nil {
		return fmt.Errorf("get table flag: %v", err)
	}

	var cfg config.TClientConfig

	if err := common.NewConfigFromPrototextFile[*config.TClientConfig](configPath, &cfg); err != nil {
		return fmt.Errorf("unknown instance: %w", err)
	}

	logger := common.NewDefaultLogger()

	if err := callServer(logger, &cfg, tableName); err != nil {
		return fmt.Errorf("call server: %w", err)
	}

	return nil
}

func callServer(logger *zap.Logger, cfg *config.TClientConfig, tableName string) error {
	cl, err := common.NewClientBufferingFromClientConfig(logger, cfg)
	if err != nil {
		return fmt.Errorf("new client buffering from client config: %w", err)
	}

	defer cl.Close()

	var splits []*api_service_protos.TSplit

	switch cfg.DataSourceInstance.Kind {
	case api_common.EDataSourceKind_CLICKHOUSE, api_common.EDataSourceKind_POSTGRESQL, api_common.EDataSourceKind_YDB:
		typeMappingSettings := &api_service_protos.TTypeMappingSettings{
			DateTimeFormat: api_service_protos.EDateTimeFormat_YQL_FORMAT,
		}

		splits, err = prepareSplits(logger, cl, cfg.DataSourceInstance, typeMappingSettings, tableName)

		if err != nil {
			return fmt.Errorf("prepare splits: %w", err)
		}
	default:
		return fmt.Errorf("unexpected data source kind %v", cfg.DataSourceInstance.Kind)
	}

	// ReadSplits
	if err := readSplits(logger, cl, splits); err != nil {
		return fmt.Errorf("read splits: %w", err)
	}

	return nil
}

func prepareSplits(
	logger *zap.Logger,
	cl *common.ClientBuffering,
	dsi *api_common.TDataSourceInstance,
	typeMappingSettings *api_service_protos.TTypeMappingSettings,
	tableName string,
) ([]*api_service_protos.TSplit, error) {
	logger.Debug("Describing table", zap.String("data_source_instance", dsi.String()))

	// DescribeTable
	describeTableResponse, err := cl.DescribeTable(context.TODO(), dsi, typeMappingSettings, tableName)
	if err != nil {
		return nil, fmt.Errorf("describe table: %w", err)
	}

	if !common.IsSuccess(describeTableResponse.Error) {
		return nil, fmt.Errorf("describe table: %v", describeTableResponse.Error)
	}

	logger.Info("Table scheme", zap.String("result", describeTableResponse.Schema.String()))

	// ListSplits - we want to SELECT *
	slct := &api_service_protos.TSelect{
		DataSourceInstance: dsi,
		What:               common.SchemaToSelectWhatItems(describeTableResponse.Schema, nil),
		From: &api_service_protos.TSelect_TFrom{
			Table: tableName,
		},
	}

	logger.Debug("Listing splits", zap.String("select", slct.String()))

	listSplitsResponse, err := cl.ListSplits(context.TODO(), slct)
	if err != nil {
		return nil, fmt.Errorf("list splits: %w", err)
	}

	logger.Info("Splits list", zap.Any("splits", listSplitsResponse))

	return common.ListSplitsResponsesToSplits(listSplitsResponse), nil
}

func readSplits(
	logger *zap.Logger,
	cl *common.ClientBuffering,
	splits []*api_service_protos.TSplit,
) error {
	logger.Debug("Reading splits")

	readSplitsResponses, err := cl.ReadSplits(context.Background(), splits)
	if err != nil {
		return fmt.Errorf("read splits: %w", err)
	}

	logger.Debug("Obtained read splits responses", zap.Int("count", len(readSplitsResponses)))

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
	for _, record := range records {
		for i, column := range record.Columns() {
			logger.Debug("column", zap.Int("id", i), zap.String("data", column.String()))
		}
	}
}
