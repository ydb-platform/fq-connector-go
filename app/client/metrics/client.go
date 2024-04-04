package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

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

func getJSON(client *http.Client, url string, target interface{}) error {
	r, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}

	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
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
	client := &http.Client{Timeout: 10 * time.Second}

	url, err := buildURL(cfg)
	if err != nil {
		return fmt.Errorf("build URL: %w", err)
	}

	var target map[string]any
	if err := getJSON(client, url, &target); err != nil {
		return fmt.Errorf("get JSON: %w", err)
	}

	fmt.Println(target)
	return nil
}
