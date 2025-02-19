package connector

import (
	"context"
	"flag"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

func listSplits(cmd *cobra.Command, _ []string) error {
	configPath, err := cmd.Flags().GetString(configFlag)
	if err != nil {
		return fmt.Errorf("get config flag: %v", err)
	}

	tableName, err := cmd.Flags().GetString(tableFlag)
	if err != nil {
		return fmt.Errorf("get table flag: %v", err)
	}

	var cfg config.TClientConfig

	if err = common.NewConfigFromPrototextFile[*config.TClientConfig](configPath, &cfg); err != nil {
		return fmt.Errorf("unknown instance: %w", err)
	}

	logger := common.NewDefaultLogger()

	flag.Parse()

	// override credentials if IAM-token provided
	common.MaybeInjectTokenToDataSourceInstance(cfg.DataSourceInstance)

	cl, err := common.NewClientBufferingFromClientConfig(logger, &cfg)
	if err != nil {
		return fmt.Errorf("new client buffering from client config: %w", err)
	}

	defer cl.Close()

	// ListSplits - we want to SELECT *
	slct := &api_service_protos.TSelect{
		DataSourceInstance: cfg.DataSourceInstance,
		From: &api_service_protos.TSelect_TFrom{
			Table: tableName,
		},
	}

	logger.Debug("Listing splits", zap.String("select", slct.String()))

	listSplitsResponse, err := cl.ListSplits(context.Background(), slct)
	if err != nil {
		return fmt.Errorf("list splits: %w", err)
	}

	for responseId, resp := range listSplitsResponse {
		for splitId, split := range resp.Splits {
			logger.Info(
				"Table split",
				zap.Int("response_id", responseId),
				zap.Int("split_id", splitId),
				zap.Any("split", split),
			)
		}
	}

	return nil
}
