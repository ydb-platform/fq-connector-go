package common

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/hashicorp/go-retryablehttp"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

// MetricsSnapshot is an instant view of statistics provided by the `fq-connector-go` service
type MetricsSnapshot struct {
	data map[string]any
}

type StatusSensor struct {
	Method string
	Name   string
	Status string
	Value  float64
}

func (ms *MetricsSnapshot) FindStatusSensors(typ, method, name, status string) []StatusSensor {
	metrics, ok := ms.data["metrics"].([]any)
	if !ok {
		panic("invalid response")
	}

	var out []StatusSensor

	for _, itemUntyped := range metrics {
		item, ok := itemUntyped.(map[string]any)
		if !ok {
			panic("invalid response")
		}

		labels, ok := item["labels"].(map[string]any)
		if !ok {
			panic("invalid response")
		}

		if item["type"] == typ && labels["name"] == name && labels["status"] == status {
			actualMethod := strings.TrimPrefix(labels["endpoint"].(string), "/NYql.NConnector.NApi.Connector/")
			if method == actualMethod {
				out = append(out, StatusSensor{
					Method: labels["endpoint"].(string),
					Name:   labels["name"].(string),
					Status: labels["status"].(string),
					Value:  item["value"].(float64),
				})
			}
		}
	}

	return out
}

func (ms *MetricsSnapshot) FindFloat64Sensor(name string) (float64, error) {
	metrics, ok := ms.data["metrics"].([]any)
	if !ok {
		return 0, fmt.Errorf("invalid response: metrics field not found or not an array")
	}

	for _, itemUntyped := range metrics {
		item, ok := itemUntyped.(map[string]any)
		if !ok {
			continue
		}

		labels, ok := item["labels"].(map[string]any)
		if !ok {
			continue
		}

		if labels["name"] != name {
			continue
		}

		value, ok := item["value"].(float64)
		if !ok {
			return 0, fmt.Errorf("sensor %q found but value is not float64", name)
		}

		return value, nil
	}

	return 0, fmt.Errorf("sensor %q not found", name)
}

func getJSON(u url.URL, target any) error {
	r, err := retryablehttp.Get(u.String())
	if err != nil {
		return fmt.Errorf("GET %s: %w", u.String(), err)
	}

	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}

func buildURL(endpoint *api_common.TGenericEndpoint, useTLS bool) url.URL {
	var u url.URL

	if useTLS {
		u.Scheme = "https"
	} else {
		u.Scheme = "http"
	}

	u.Host = EndpointToString(endpoint)
	u.Path = "metrics"

	return u
}

func NewMetricsSnapshot(endpoint *api_common.TGenericEndpoint, useTLS bool) (*MetricsSnapshot, error) {
	mp := &MetricsSnapshot{}

	if err := getJSON(buildURL(endpoint, useTLS), &mp.data); err != nil {
		return nil, fmt.Errorf("get JSON: %w", err)
	}

	return mp, nil
}

func DiffStatusSensors(oldSnapshot, newSnapshot *MetricsSnapshot, typ, method, name, status string) (float64, error) {
	var oldValue float64

	oldSensors := oldSnapshot.FindStatusSensors(typ, method, name, status)
	switch len(oldSensors) {
	case 0:
		oldValue = 0 // happens if service hasn't handled request since start
	case 1:
		oldValue = oldSensors[0].Value
	default:
		return 0, fmt.Errorf("unexpected number of sensors in old snapshot: %d", len(oldSensors))
	}

	newSensors := newSnapshot.FindStatusSensors(typ, method, name, status)
	if len(newSensors) != 1 {
		return 0, fmt.Errorf("unexpected number of sensors in new snapshot: %d", len(newSensors))
	}

	return newSensors[0].Value - oldValue, nil
}
