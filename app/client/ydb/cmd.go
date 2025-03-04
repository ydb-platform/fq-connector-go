package ydb

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "ydb",
	Short: "Client for YDB",
}

var columnShardsDataDistributionCmd = &cobra.Command{
	Use:   "cs_data_distribution",
	Short: "Estimation of data distribution across column shards",
	Run: func(cmd *cobra.Command, args []string) {
		if err := columnShardsDataDistribution(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

var columnShardBenchmarkSelectCmd = &cobra.Command{
	Use:   "cs_benchmark_select",
	Short: "Benchmark speed of a `SELECT * from table` query with columnar table",
	Run: func(cmd *cobra.Command, args []string) {
		if err := columnShardBenchmarkSelect(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

const (
	configFlag = "config"
	tableFlag  = "table"
)

func init() {
	Cmd.AddCommand(columnShardsDataDistributionCmd)
	Cmd.AddCommand(columnShardBenchmarkSelectCmd)

	Cmd.Flags().StringP(configFlag, "c", "", "path to server config file")

	if err := Cmd.MarkFlagRequired(configFlag); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	Cmd.Flags().StringP(tableFlag, "t", "", "table to read")

	if err := Cmd.MarkFlagRequired(tableFlag); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// inherit parent flags
	columnShardsDataDistributionCmd.Flags().AddFlagSet(Cmd.Flags())
	columnShardBenchmarkSelectCmd.Flags().AddFlagSet(Cmd.Flags())
}
