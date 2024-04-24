package version

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var (
	Tag           string
	Author        string
	CommitHash    string
	Branch        string
	CommitDate    string
	CommitMessage string
	Username      string
	BuildLocation string
	Hostname      string
	HostInfo      string
	PathToGo      string
	GoVersion     string
)

var Cmd = &cobra.Command{
	Use:   "version",
	Short: "version of current build",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(GetInfo())
	},
}

func GetInfo() string {
	sb := strings.Builder{}

	sb.WriteString("Git info:\n")
	sb.WriteString(fmt.Sprintf("\tBranch: %s\n", Branch))
	sb.WriteString(fmt.Sprintf("\tCommit: %s\n", CommitHash))
	sb.WriteString(fmt.Sprintf("\tTag: %s\n", Tag))
	sb.WriteString(fmt.Sprintf("\tAuthor: %s\n", Author))
	sb.WriteString(fmt.Sprintf("\tSummary: %s\n", CommitMessage))
	sb.WriteString(fmt.Sprintf("\tCommit Date: %s\n\n", CommitDate))
	sb.WriteString("Other info:\n")
	sb.WriteString(fmt.Sprintf("\tBuilt by: %s\n", Username))
	sb.WriteString(fmt.Sprintf("\tBuilding location: %s\n", BuildLocation))
	sb.WriteString(fmt.Sprintf("\tHostname: %s\n", Hostname))
	sb.WriteString("\tHost information:\n")
	sb.WriteString(fmt.Sprintf("\t\t%s\n\n", HostInfo))
	sb.WriteString("Build info:\n")
	sb.WriteString(fmt.Sprintf("\tCompiler: %s\n", PathToGo))
	sb.WriteString("\tCompiler version:\n")
	sb.WriteString(fmt.Sprintf("\t\t%s\n", GoVersion))

	return sb.String()
}
