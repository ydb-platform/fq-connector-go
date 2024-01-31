package server

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

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

const configFlag = "config"

func init() {
	Cmd.Flags().StringP(configFlag, "c", "", "path to server config file")

	if err := Cmd.MarkFlagRequired(configFlag); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runFromCLI(cmd *cobra.Command, _ []string) error {
	configPath, err := cmd.Flags().GetString(configFlag)
	if err != nil {
		return fmt.Errorf("get config flag: %v", err)
	}

	cfg, err := newConfigFromPath(configPath)
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
