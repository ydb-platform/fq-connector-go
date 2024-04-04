package metrics

import (
	"fmt"
	"net/url"

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

func buildURL(cfg *config.TClientConfig) (string, error) {
	if cfg.MetricsServerEndpoint == nil {
		return "", fmt.Errorf("empty metrics_server_endpoint field")
	}

	var url url.URL

	url.Scheme = "http"
	url.Host = common.EndpointToString(cfg.MetricsServerEndpoint)
	url.Path = "metrics"

	return url.String(), nil
}

func callServer(logger *zap.Logger, cfg *config.TClientConfig) error {
	url, err := buildURL(cfg)
	if err != nil {
		return fmt.Errorf("build URL: %w", err)
	}

	mp, err := common.NewMetricsProvider(url)
	if err != nil {
		return fmt.Errorf("new metrics provider: %w", err)
	}

	result, err := mp.Find("RATE", "status_total")
	if err != nil {
		return fmt.Errorf("find: %w", err)
	}

	fmt.Println(result)

	return nil
}
