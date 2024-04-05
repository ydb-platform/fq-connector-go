package metrics

import (
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

func runClient(cmd *cobra.Command, _ []string) error {
	configPath, err := cmd.Flags().GetString(configFlag)
	if err != nil {
		return fmt.Errorf("get config flag: %v", err)
	}

	var cfg config.TClientConfig

	if err := common.NewConfigFromPrototextFile[*config.TClientConfig](configPath, &cfg); err != nil {
		return fmt.Errorf("unknown instance: %w", err)
	}

	logger := common.NewDefaultLogger()

	if err := callServer(logger, &cfg); err != nil {
		return fmt.Errorf("call server: %w", err)
	}

	return nil
}

func callServer(_ *zap.Logger, cfg *config.TClientConfig) error {
	mp, err := common.NewMetricsSnapshot(cfg.MetricsServerEndpoint, false)
	if err != nil {
		return fmt.Errorf("new metrics provider: %w", err)
	}

	result := mp.FindStatusSensors("RATE", "status_total", "OK")

	fmt.Println(result)

	return nil
}
