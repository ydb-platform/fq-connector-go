package common

import (
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

func EndpointToString(ep *api_common.TGenericEndpoint) string {
	return fmt.Sprintf("%s:%d", ep.GetHost(), ep.GetPort())
}

func StringToEndpoint(s string) (*api_common.TGenericEndpoint, error) {
	var (
		host string
		port uint32
	)

	if _, err := fmt.Sscanf(s, "%s:%d", &host, &port); err != nil {
		return nil, fmt.Errorf("parse endpoint '%s': %w", s, err)
	}

	return &api_common.TGenericEndpoint{
		Host: host,
		Port: port,
	}, nil
}
