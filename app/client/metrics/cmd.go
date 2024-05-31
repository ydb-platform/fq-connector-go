package metrics

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const configFlag = "config"

func init() {
	Cmd.Flags().StringP(configFlag, "c", "", "path to server config file")

	if err := Cmd.MarkFlagRequired(configFlag); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var Cmd = &cobra.Command{
	Use:   "metrics",
	Short: "Client for Solomon HTTP API",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runClient(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}
