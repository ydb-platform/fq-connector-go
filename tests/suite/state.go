package suite

import (
	"flag"
	"fmt"
	"os"

	"github.com/ydb-platform/fq-connector-go/tests/infra/docker_compose"
)

// CLI parameters
var projectPath = flag.String("projectPath", "", "path to fq-connector-go source dir")

// Global-scope services and objects that will be accessible from every test suite
// during the testing lifecycle
type State struct {
	EndpointDeterminer *docker_compose.EndpointDeterminer
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
	}

	return result, nil
}
