package suite

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/docker_compose"
)

// CLI parameters
var (
	projectPath = flag.String("projectPath", "", "path to fq-connector-go source dir")
	suiteName   = flag.String("suiteName", "", "specifies the test suite one wants to run")
)

// Global-scope services and objects that will be accessible from every test suite
// during the testing lifecycle
type State struct {
	EndpointDeterminer *docker_compose.EndpointDeterminer
	suiteName          string
}

func (s *State) SkipSuiteIfNotEnabled(t *testing.T) {
	if s.suiteName == "" {
		// if no suite specified, run all suites
		return
	}

	functionNames := common.GetCallStackFunctionNames()
	if len(functionNames) == 0 {
		t.FailNow()
		return
	}

	for _, functionName := range functionNames {
		if strings.Contains(functionName, "Test") {
			actualSuiteName := strings.TrimLeft(strings.Split(functionName, ".")[2], "Test")
			if actualSuiteName == s.suiteName {
				return
			}

			log.Printf("Suite '%s' skipped as it doesn't match flag value '%s'\n", actualSuiteName, s.suiteName)
			t.SkipNow()

			return
		}
	}
}

func NewState() (*State, error) {
	flag.Parse()

	if *projectPath == "" {
		return nil, fmt.Errorf("empty projectPath parameter")
	}

	projectPathInfo, err := os.Stat(*projectPath)
	if err != nil {
		return nil, fmt.Errorf("cannot check projectPath '%v': %w", *projectPath, err)
	}

	if !projectPathInfo.IsDir() {
		return nil, fmt.Errorf("projectPath '%v' is not a directory", *projectPath)
	}

	ed, err := docker_compose.NewEndpointDeterminer(*projectPath)
	if err != nil {
		return nil, fmt.Errorf("new endpoint determiner: %w", err)
	}

	result := &State{
		EndpointDeterminer: ed,
		suiteName:          *suiteName,
	}

	return result, nil
}
