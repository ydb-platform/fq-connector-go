package docker_compose

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

type EndpointDeterminer struct {
	dockerComposeFile string
}

func (ed *EndpointDeterminer) GetEndpoint(service string, internalPort int) (*api_common.TEndpoint, error) {
	cmd := "docker-compose"
	args := []string{
		"-f",
		ed.dockerComposeFile,
		"port",
		service,
		fmt.Sprint(internalPort),
	}

	out, err := exec.Command(cmd, args...).CombinedOutput()
	if err != nil {
		cmdStr := fmt.Sprintf("%s %s", cmd, strings.Join(args, " "))
		return nil, fmt.Errorf("exec cmd '%v': %w\n%s", cmdStr, err, string(out))
	}

	host, portStr, err := net.SplitHostPort(string(out))
	if err != nil {
		return nil, fmt.Errorf("split '%s' to host and port: %w", string(out), err)
	}

	portStr = strings.TrimSpace(portStr)

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("convert '%s' to int: %w", portStr, err)
	}

	return &api_common.TEndpoint{
		Host: host,
		Port: uint32(port),
	}, nil
}

func NewEndpointDeterminer(projectPath string) (*EndpointDeterminer, error) {
	dockerComposeFile := filepath.Join(projectPath, "tests/infra/datasource/docker-compose.yaml")

	_, err := os.Stat(dockerComposeFile)
	if err != nil {
		return nil, fmt.Errorf("cannot check docker_compose file '%v': %w", dockerComposeFile, err)
	}

	return &EndpointDeterminer{
		dockerComposeFile: dockerComposeFile,
	}, nil
}
