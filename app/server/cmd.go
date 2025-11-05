package server

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
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
	configFlag                = "config"
	connectorPort             = "connector-port"
	metricsPort               = "metrics-port"
	pprofPort                 = "pprof-port"
	logLevelFlag              = "log-level"
	enableSQLQueryLoggingFlag = "enable-sql-query-logging"
)

var (
	enableSQLQueryLogging bool
)

func init() {
	Cmd.Flags().StringP(configFlag, "c", "", "Path to server config file")
	Cmd.Flags().Uint32(connectorPort, 2130, "Connector GRPC server port")
	Cmd.Flags().Uint32(metricsPort, 8766, "Metrics HTTP server port")
	Cmd.Flags().Uint32(pprofPort, 6060, "Go pprof HTTP server port")
	Cmd.Flags().StringP(logLevelFlag, "l", "DEBUG", "Log level")
	Cmd.Flags().BoolVar(&enableSQLQueryLogging, enableSQLQueryLoggingFlag, false,
		"Enable user SQL query logging (may dump private information in logs)")

	if err := Cmd.MarkFlagRequired(configFlag); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func flagToPort(f *pflag.Flag, port *uint32, errs *[]error) {
	val, err := strconv.Atoi(f.Value.String())
	if err != nil {
		*errs = append(*errs, fmt.Errorf("strconv '%s': %w", f.Value, err))

		return
	}

	*port = uint32(val)
}

func overrideConfigWithFlags(cfg *app_config.TServerConfig, flags *pflag.FlagSet) error {
	var errs []error

	flags.Visit(func(f *pflag.Flag) {
		switch f.Name {
		case connectorPort:
			flagToPort(f, &cfg.ConnectorServer.Endpoint.Port, &errs)
		case metricsPort:
			flagToPort(f, &cfg.MetricsServer.Endpoint.Port, &errs)
		case pprofPort:
			if cfg.PprofServer == nil {
				cfg.PprofServer = &app_config.TPprofServerConfig{
					Endpoint: &api_common.TGenericEndpoint{},
				}
			}

			flagToPort(f, &cfg.PprofServer.Endpoint.Port, &errs)
		case logLevelFlag:
			val, ok := app_config.ELogLevel_value[f.Value.String()]
			if !ok {
				var buf strings.Builder

				for k := range app_config.ELogLevel_value {
					buf.WriteString(fmt.Sprintf("\"%s\"", k))
					buf.WriteString(" ")
				}

				errs = append(
					errs,
					fmt.Errorf("unknown log level '%s', valid levels are: %s", f.Value.String(), buf.String()),
				)

				return
			}

			cfg.Logger.LogLevel = app_config.ELogLevel(val)
		case enableSQLQueryLoggingFlag:
			cfg.Logger.EnableSqlQueryLogging = enableSQLQueryLogging
		default:
		}
	})

	return errors.Join(errs...)
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

	if err = overrideConfigWithFlags(cfg, cmd.Flags()); err != nil {
		return fmt.Errorf("override config with flags: %w", err)
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
