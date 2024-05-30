package version

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var (
	tag           string
	author        string
	commitHash    string
	branch        string
	commitDate    string
	commitMessage string
	username      string
	buildLocation string
	hostname      string
	hostInfo      string
	pathToGo      string
	goVersion     string
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

	sb.WriteString(`Git info:\n`)
	sb.WriteString(fmt.Sprintf(`\tBranch: %s\n`, branch))
	sb.WriteString(fmt.Sprintf(`\tCommit: %s\n`, commitHash))
	sb.WriteString(fmt.Sprintf(`\tTag: %s\n`, tag))
	sb.WriteString(fmt.Sprintf(`\tAuthor: %s\n`, author))
	sb.WriteString(fmt.Sprintf(`\tSummary: %s\n`, commitMessage))
	sb.WriteString(fmt.Sprintf(`\tCommit Date: %s\n\n`, commitDate))
	sb.WriteString(`Other info:\n`)
	sb.WriteString(fmt.Sprintf(`\tBuilt by: %s\n`, username))
	sb.WriteString(fmt.Sprintf(`\tBuilding location: %s\n`, buildLocation))
	sb.WriteString(fmt.Sprintf(`\tHostname: %s\n`, hostname))
	sb.WriteString(`\tHost information:\n`)
	sb.WriteString(fmt.Sprintf(`\t\t%s\n\n`, hostInfo))
	sb.WriteString(`Build info:\n`)
	sb.WriteString(fmt.Sprintf(`\tCompiler: %s\n`, pathToGo))
	sb.WriteString(`\tCompiler version:\n`)
	sb.WriteString(fmt.Sprintf(`\t\t%s\n`, goVersion))

	return sb.String()
}
