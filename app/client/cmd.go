package client

import (
	"github.com/spf13/cobra"

	"github.com/ydb-platform/fq-connector-go/app/client/connector"
	"github.com/ydb-platform/fq-connector-go/app/client/metrics"
	"github.com/ydb-platform/fq-connector-go/app/client/observation"
	"github.com/ydb-platform/fq-connector-go/app/client/ydb"
)

var Cmd = &cobra.Command{
	Use:   "client",
	Short: "Client for various services working within fq-connector-go process",
}

func init() {
	Cmd.AddCommand(connector.Cmd)
	Cmd.AddCommand(metrics.Cmd)
	Cmd.AddCommand(observation.Cmd)
	Cmd.AddCommand(ydb.Cmd)
}
