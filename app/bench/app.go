package bench

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

func validateConfig(logger *zap.Logger, cfg *config.TBenchmarkConfig) error {
	if cfg.GetServerRemote() == nil && cfg.GetServerLocal() == nil {
		return fmt.Errorf("you must provide either local or remote configuration for connector")
	}

	if cfg.GetDataSourceInstance() == nil {
		return fmt.Errorf("you must provide data source instance")
	}

	if cfg.GetResultDir() == "" {
		return fmt.Errorf("empty result dir")
	}

	if _, err := os.Stat(cfg.GetResultDir()); os.IsNotExist(err) {
		logger.Debug("trying to create directory", zap.String("path", cfg.GetResultDir()))

		if err := os.MkdirAll(cfg.GetResultDir(), 0700); err != nil {
			return fmt.Errorf("make directory %s: %w", cfg.GetResultDir(), err)
		}
	}

	return nil
}

func runBenchmarks(_ *cobra.Command, args []string) error {
	var (
		configPath = args[0]
		cfg        config.TBenchmarkConfig
		logger     = common.NewDefaultLogger()
	)

	if err := common.NewConfigFromPrototextFile[*config.TBenchmarkConfig](configPath, &cfg); err != nil {
		return fmt.Errorf("new config from prototext file '%s': %w", configPath, err)
	}

	if err := validateConfig(logger, &cfg); err != nil {
		return fmt.Errorf("validate config: %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)

	go func() {
		errChan <- runTestCases(ctx, logger, &cfg)
	}()

	var err error

	select {
	case sig := <-signalChan:
		logger.Info("interrupting signal", zap.Any("value", sig))
		cancel() // make requests terminate

		err = <-errChan // wait for goroutine to stop
	case err = <-errChan:
	}

	if err != nil {
		return fmt.Errorf("run test cases: %w", err)
	}

	return nil
}

func runTestCases(ctx context.Context, logger *zap.Logger, cfg *config.TBenchmarkConfig) error {
	for i, tc := range cfg.TestCases {
		// prepare test case
		tcr, err := newTestCaseRunner(ctx, logger, cfg, tc)
		if err != nil {
			return fmt.Errorf("new test case runner: %w", err)
		}

		fmt.Println(">>>>>>>>>>>>>>>>>>> " + tcr.name() + " <<<<<<<<<<<<<<<<<<<<")

		// run it
		if err := tcr.run(); err != nil {
			return fmt.Errorf("failed to run test case #%d: %w", i, err)
		}

		report := tcr.finish()

		if err := report.saveToFile(filepath.Join(cfg.ResultDir, tcr.name()+".json")); err != nil {
			return fmt.Errorf("failed to save report #%d: %w", i, err)
		}
	}

	return nil
}

var Cmd = &cobra.Command{
	Use:   "bench",
	Short: "Benchmarking tool to test performance of Connector installations",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runBenchmarks(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}
