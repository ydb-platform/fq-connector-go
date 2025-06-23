package discovery

import (
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
)

type staticDiscovery struct {
	cfg *config.TObservationDiscoveryConfig_TStaticDiscoveryConfig
}

func (d *staticDiscovery) GetEndpoints(_ *zap.Logger) ([]*api_common.TGenericEndpoint, error) {
	return d.cfg.Endpoints, nil
}

func newStaticDiscovery(cfg *config.TObservationDiscoveryConfig_TStaticDiscoveryConfig) Discovery {
	return &staticDiscovery{cfg: cfg}
}
