package connector

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const (
	configFlag         = "config"
	tableFlag          = "table"
	dateTimeFormatFlag = "date-time-format"
	userIDFlag         = "user-id"
	sessionIDFlag      = "session"
)

func init() {
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

	Cmd.Flags().StringP(userIDFlag, "u", "", "user-id")
	Cmd.Flags().StringP(sessionIDFlag, "s", "", "flag-id")
	Cmd.Flags().StringP(dateTimeFormatFlag, "d", "YQL_FORMAT", "date-time-format")
}

var Cmd = &cobra.Command{
	Use:   "connector",
	Short: "Client for Connector GRPC API",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runClient(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}
