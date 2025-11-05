package discovery

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	discovery_v1 "k8s.io/api/discovery/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
)

func getCurrentNamespace() (string, error) {
	const nsPath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	nsBytes, err := os.ReadFile(nsPath)
	if err != nil {
		return "", fmt.Errorf("read file '%s': %v", nsPath, err)
	}

	return strings.TrimSpace(string(nsBytes)), nil
}

type kubernetesDiscovery struct {
	// kubernetes.io/service-name=yq-connector
	cfg *config.TObservationDiscoveryConfig_TKubernetesDiscoveryConfig
}

// extractEndpointsFromSlice converts Kubernetes EndpointSlice to TGenericEndpoint list
func (k *kubernetesDiscovery) extractEndpointsFromSlice(
	logger *zap.Logger,
	slice *discovery_v1.EndpointSlice,
) []*api_common.TGenericEndpoint {
	var endpoints []*api_common.TGenericEndpoint

	for _, endpoint := range slice.Endpoints {
		// Skip endpoints that are not ready
		if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
			continue
		}

		for _, port := range slice.Ports {
			if port.Port == nil {
				continue
			}

			if *port.Port != int32(k.cfg.TargetPort) {
				logger.Debug("skipping endpoint with non-target port", zap.Int32("port", *port.Port))

				continue
			}

			for _, address := range endpoint.Addresses {
				logger.Debug("adding endpoint", zap.String("address", address), zap.Int32("port", *port.Port))

				// Create a TGenericEndpoint for each address and port combination
				endpoint := &api_common.TGenericEndpoint{
					Host: address,
					Port: uint32(*port.Port),
				}

				endpoints = append(endpoints, endpoint)
			}
		}
	}

	return endpoints
}

// GetEndpoints retrieves endpoints from Kubernetes API based on namespace and labelSelector
func (k *kubernetesDiscovery) GetEndpoints(logger *zap.Logger) ([]*api_common.TGenericEndpoint, error) {
	// Use in-cluster cfg for Kubernetes client
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("rest in-cluster config: %w", err)
	}

	clientSet, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes new for config: %w", err)
	}

	// Use provided namespace or get current namespace
	namespace, err := getCurrentNamespace()
	if err != nil {
		return nil, fmt.Errorf("get current namespace: %w", err)
	}

	// List EndpointSlices with the given label selector
	endpointSlices, err := clientSet.DiscoveryV1().EndpointSlices(namespace).List(
		context.Background(),
		meta_v1.ListOptions{
			LabelSelector: k.cfg.LabelSelector,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("list endpoint slices: %w", err)
	}

	// Collect all endpoints from all slices
	var allEndpoints []*api_common.TGenericEndpoint

	for _, slice := range endpointSlices.Items {
		endpoints := k.extractEndpointsFromSlice(logger, &slice)

		allEndpoints = append(allEndpoints, endpoints...)
	}

	return allEndpoints, nil
}

func newKubernetesDiscovery(cfg *config.TObservationDiscoveryConfig_TKubernetesDiscoveryConfig) Discovery {
	return &kubernetesDiscovery{
		cfg: cfg,
	}
}
