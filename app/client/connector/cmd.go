package connector

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/ydb-platform/fq-connector-go/app/client/utils"
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

func init() {
	Cmd.AddCommand(readTableCmd)
	Cmd.AddCommand(listSplitsCmd)

	Cmd.Flags().StringP(utils.ConfigFlag, "c", "", "path to client config file")

	if err := Cmd.MarkFlagRequired(utils.ConfigFlag); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	Cmd.Flags().StringP(utils.TableFlag, "t", "", "table to read")

	if err := Cmd.MarkFlagRequired(utils.TableFlag); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// inherit parent flags
	readTableCmd.Flags().AddFlagSet(Cmd.Flags())
	listSplitsCmd.Flags().AddFlagSet(Cmd.Flags())

	readTableCmd.Flags().StringP(utils.UserIDFlag, "u", "", "user-id")
	readTableCmd.Flags().StringP(utils.SessionIDFlag, "s", "", "flag-id")
	readTableCmd.Flags().StringP(utils.DateTimeFormatFlag, "", "YQL_FORMAT", "date-time-format")
}
