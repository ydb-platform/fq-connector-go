package discovery

import "github.com/ydb-platform/fq-connector-go/app/config"

func NewDiscovery(cfg *config.TObservationDiscoveryConfig) (Discovery, error) {
	switch t := cfg.GetDiscoveryConfig().(type) {
	case *config.TObservationDiscoveryConfig_Static:
		return newStaticDiscovery(t.Static), nil
	case *config.TObservationDiscoveryConfig_Kubernetes:
		return newKubernetesDiscovery(t.Kubernetes), nil
	default:
		return nil, nil
	}
}
