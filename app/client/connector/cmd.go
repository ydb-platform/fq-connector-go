package connector

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "connector",
	Short: "Client for Connector GRPC API",
}

var readTableCmd = &cobra.Command{
	Use:   "read_table",
	Short: "Read table from the external data source",
	Run: func(cmd *cobra.Command, args []string) {
		if err := readTable(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

var listSplitsCmd = &cobra.Command{
	Use:   "list_splits",
	Short: "List splits for the table in the external data source",
	Run: func(cmd *cobra.Command, args []string) {
		if err := listSplits(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

const (
	configFlag         = "config"
	tableFlag          = "table"
	dateTimeFormatFlag = "date-time-format"
	userIDFlag         = "user-id"
	sessionIDFlag      = "session"
)

func init() {
	Cmd.AddCommand(readTableCmd)
	Cmd.AddCommand(listSplitsCmd)

	Cmd.Flags().StringP(configFlag, "c", "", "path to client config file")

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
	readTableCmd.Flags().AddFlagSet(Cmd.Flags())
	listSplitsCmd.Flags().AddFlagSet(Cmd.Flags())

	readTableCmd.Flags().StringP(userIDFlag, "u", "", "user-id")
	readTableCmd.Flags().StringP(sessionIDFlag, "s", "", "flag-id")
	readTableCmd.Flags().StringP(dateTimeFormatFlag, "", "YQL_FORMAT", "date-time-format")
}
