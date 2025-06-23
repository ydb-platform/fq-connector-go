package discovery

import (
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

type Discovery interface {
	// Returns the list of Observation API endpoints
	GetEndpoints(*zap.Logger) ([]*api_common.TGenericEndpoint, error)
}
