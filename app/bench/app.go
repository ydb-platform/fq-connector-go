package bench

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

func validateConfig(cfg *config.TBenchmarkConfig) error {
	if cfg.GetServerRemote() != nil {
		return fmt.Errorf("not ready to work with remote connector")
	}

	if cfg.GetServerLocal() == nil {
		return fmt.Errorf("you must provide local configuration for connector")
	}

	if cfg.GetDataSourceInstance() == nil {
		return fmt.Errorf("you must provide data source instance")
	}

	if cfg.GetResultDir() == "" {
		return fmt.Errorf("empty result dir")
	}

	return nil
}

func runBenchmarks(_ *cobra.Command, args []string) error {
	var (
		configPath = args[0]
		cfg        config.TBenchmarkConfig
	)

	if err := common.NewConfigFromPrototextFile[*config.TBenchmarkConfig](configPath, &cfg); err != nil {
		return fmt.Errorf("new config from prototext file '%s': %w", configPath, err)
	}

	if err := validateConfig(&cfg); err != nil {
		return fmt.Errorf("validate config: %v", err)
	}

	logger := common.NewDefaultLogger()

	// prepare test case runners
	testCasesRunners := make([]*testCaseRunner, 0, len(cfg.TestCases))
	for _, tc := range cfg.TestCases {
		tcr, err := newTestCaseRunner(logger, &cfg, tc)
		if err != nil {
			return fmt.Errorf("new test case runner: %w", err)
		}

		testCasesRunners = append(testCasesRunners, tcr)
	}

	// and run them
	for i, tcr := range testCasesRunners {
		if err := tcr.run(); err != nil {
			return fmt.Errorf("failed to run test case #%d: %w", i, err)
		}

		report := tcr.finish()

		if err := report.saveToFile(cfg.ResultDir); err != nil {
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
