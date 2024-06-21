package server

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	app_config "github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

var Cmd = &cobra.Command{
	Use:   "server",
	Short: "Connector server",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runFromCLI(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

const (
	configFlag    = "config"
	connectorPort = "connector-port"
	metricsPort   = "metrics-port"
	pprofPort     = "pprof-port"
)

func init() {
	Cmd.Flags().StringP(configFlag, "c", "", "Path to server config file")
	Cmd.Flags().Uint32(connectorPort, 2130, "Connector GRPC server port")
	Cmd.Flags().Uint32(metricsPort, 8766, "Metrics HTTP server port")
	Cmd.Flags().Uint32(pprofPort, 6060, "Go pprof HTTP server port")

	if err := Cmd.MarkFlagRequired(configFlag); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func flagToUint32() {

}

func overrideConfigWithFlags(cfg *app_config.TServerConfig, flags *pflag.FlagSet) error {
	flags.Visit(func(f *pflag.Flag) {
		var err error
		switch f.Name {
		case connectorPort:
			cfg.ConnectorServer.Endpoint.Port, err = strconv.Atoi(f.Value.String())
		case metricsPort:
		case pprofPort:
		}
	})
}

func runFromCLI(cmd *cobra.Command, _ []string) error {
	configPath, err := cmd.Flags().GetString(configFlag)
	if err != nil {
		return fmt.Errorf("get config flag: %v", err)
	}

	cfg, err := config.NewConfigFromFile(configPath)
	if err != nil {
		return fmt.Errorf("new config: %w", err)
	}

	logger, err := common.NewLoggerFromConfig(cfg.Logger)
	if err != nil {
		return fmt.Errorf("new logger from config: %w", err)
	}

	l, err := NewLauncher(logger, cfg)
	if err != nil {
		return fmt.Errorf("new launcher: %w", err)
	}

	startLauncherAndWaitForSignalOrError(logger, l)

	return nil
}
