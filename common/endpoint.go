package common

import (
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

func EndpointToString(ep *api_common.TGenericEndpoint) string {
	return fmt.Sprintf("%s:%d", ep.GetHost(), ep.GetPort())
}
