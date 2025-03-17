package connector

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/client/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

func listSplits(cmd *cobra.Command, _ []string) error {
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

	// ListSplits - we want to SELECT *
	slct := &api_service_protos.TSelect{
		DataSourceInstance: preset.Cfg.DataSourceInstance,
		From: &api_service_protos.TSelect_TFrom{
			Table: preset.TableName,
		},
	}

	logger := preset.Logger
	logger.Debug("Listing splits", zap.String("select", slct.String()))

	listSplitsResponse, err := client.ListSplits(context.Background(), slct)
	if err != nil {
		return fmt.Errorf("list splits: %w", err)
	}

	for responseId, resp := range listSplitsResponse {
		if !common.IsSuccess(resp.Error) {
			return common.NewSTDErrorFromAPIError(resp.Error)
		}

		for splitId, split := range resp.Splits {
			logger.Info(
				"Table split",
				zap.Int("response_id", responseId),
				zap.Int("split_id", splitId),
			)

			fmt.Println("Split select: ", common.MustProtobufToJSONString(split.Select, false, ""))
			// fq-connector-go serializes split descriptions to JSON, so they're always human-readable
			fmt.Println("Split description: ", string(split.GetDescription()))
			fmt.Printf("\n")
		}
	}

	return nil
}
