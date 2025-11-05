package observation

import (
	"errors"
	"fmt"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

func validateObservationServerConfig(cfg *config.TObservationServerConfig) error {
	if cfg.Endpoint == nil {
		return errors.New("missing required field `endpoint`")
	}

	if err := validateObservationDiscoveryConfig(cfg.Discovery); err != nil {
		return fmt.Errorf("validate `discovery`: %v", err)
	}

	if _, err := common.DurationFromString(cfg.GetPollingInterval()); err != nil {
		return fmt.Errorf("validate `polling_interval`: %v", err)
	}

	return nil
}

func validateObservationDiscoveryConfig(cfg *config.TObservationDiscoveryConfig) error {
	if cfg == nil {
		return errors.New("missing required field `discovery`")
	}

	switch t := cfg.GetPayload().(type) {
	case *config.TObservationDiscoveryConfig_Static:
		if err := validateObservationDiscoveryStaticConfig(t.Static); err != nil {
			return fmt.Errorf("validate `static`: %v", err)
		}
	case *config.TObservationDiscoveryConfig_Kubernetes:
		if err := validateObservationDiscoveryKubernetesConfig(t.Kubernetes); err != nil {
			return fmt.Errorf("validate `kubernetes`: %v", err)
		}
	default:
		return fmt.Errorf("unknown discovery type: %T", t)
	}

	return nil
}

func validateObservationDiscoveryStaticConfig(cfg *config.TObservationDiscoveryConfig_TStaticDiscoveryConfig) error {
	if len(cfg.Endpoints) == 0 {
		return errors.New("missing required field `endpoints`")
	}

	for _, endpoint := range cfg.Endpoints {
		if endpoint.GetHost() == "" {
			return errors.New("missing required field `host`")
		}

		if endpoint.GetPort() == 0 {
			return errors.New("missing required field `port`")
		}
	}

	return nil
}

func validateObservationDiscoveryKubernetesConfig(cfg *config.TObservationDiscoveryConfig_TKubernetesDiscoveryConfig) error {
	if cfg.LabelSelector == "" {
		return errors.New("missing required field `label_selector`")
	}

	return nil
}

func newConfigFromFile(configPath string) (*config.TObservationServerConfig, error) {
	var cfg config.TObservationServerConfig

	if err := common.NewConfigFromYAMLFile(configPath, &cfg); err != nil {
		return nil, fmt.Errorf("new config from YAML file '%s': %w", configPath, err)
	}

	if err := validateObservationServerConfig(&cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}
