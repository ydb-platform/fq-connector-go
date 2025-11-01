package common //nolint:revive

import (
	"fmt"
	"strconv"
	"strings"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

func EndpointToString(ep *api_common.TGenericEndpoint) string {
	host := ep.GetHost()

	// Check if this is an IPv6 address (contains colon)
	if strings.Contains(host, ":") {
		return fmt.Sprintf("[%s]:%d", host, ep.GetPort())
	}

	return fmt.Sprintf("%s:%d", host, ep.GetPort())
}

func StringToEndpoint(s string) (*api_common.TGenericEndpoint, error) {
	ss := strings.Split(s, ":")

	if len(ss) != 2 {
		return nil, fmt.Errorf("invalid endpoint format: %s", s)
	}

	port, err := strconv.ParseUint(ss[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %s", ss[1])
	}

	if port > 65535 {
		return nil, fmt.Errorf("invalid port: %s", ss[1])
	}

	return &api_common.TGenericEndpoint{
		Host: ss[0],
		Port: uint32(port),
	}, nil
}
