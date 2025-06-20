package discovery

import api_common "github.com/ydb-platform/fq-connector-go/api/common"

type Discovery interface {
	// Returns the list of Observation API endpoints
	GetEndpoints() ([]*api_common.TGenericEndpoint, error)
}
