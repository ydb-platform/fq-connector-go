package discovery

import (
	"context"
	"fmt"
	"os"
	"strings"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	discovery_v1 "k8s.io/api/discovery/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func getCurrentNamespace() (string, error) {
	nsPath := "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	nsBytes, err := os.ReadFile(nsPath)
	if err != nil {
		return "", fmt.Errorf("error reading namespace file: %v", err)
	}
	return strings.TrimSpace(string(nsBytes)), nil
}

// extractEndpointsFromSlice converts Kubernetes EndpointSlice to TGenericEndpoint list
func extractEndpointsFromSlice(slice *discovery_v1.EndpointSlice) []*api_common.TGenericEndpoint {
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

			for _, address := range endpoint.Addresses {
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

type kubernetesDiscovery struct {
	// kubernetes.io/service-name=yq-connector
	cfg *config.TObservationDiscoveryConfig_TKubernetesDiscoveryConfig
}

// GetEndpoints retrieves endpoints from Kubernetes API based on namespace and labelSelector
func (k *kubernetesDiscovery) GetEndpoints() ([]*api_common.TGenericEndpoint, error) {
	// Use in-cluster config for Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("create in-cluster config: %w", err)
	}

	// Create Kubernetes clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}

	// Use provided namespace or get current namespace
	namespace, err := getCurrentNamespace()
	if err != nil {
		return nil, fmt.Errorf("get current namespace: %w", err)
	}

	// List EndpointSlices with the given label selector
	endpointSlices, err := clientset.DiscoveryV1().EndpointSlices(namespace).List(
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
		endpoints := extractEndpointsFromSlice(&slice)
		allEndpoints = append(allEndpoints, endpoints...)
	}

	return allEndpoints, nil
}

func newKubernetesDiscovery(cfg *config.TObservationDiscoveryConfig_TKubernetesDiscoveryConfig) Discovery {
	return &kubernetesDiscovery{
		cfg: cfg,
	}
}
