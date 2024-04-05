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

	for _, item_ := range metrics {
		item, ok := item_.(map[string]any)
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

func getJSON(url string, target interface{}) error {
	r, err := retryablehttp.Get(url)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}

	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}

func buildURL(endpoint *api_common.TEndpoint, useTLS bool) (string, error) {
	var url url.URL

	if useTLS {
		url.Scheme = "https"
	} else {
		url.Scheme = "http"
	}
	url.Host = EndpointToString(endpoint)
	url.Path = "metrics"

	return url.String(), nil
}

func NewMetricsSnapshot(endpoint *api_common.TEndpoint, useTLS bool) (*MetricsSnapshot, error) {
	url, err := buildURL(endpoint, useTLS)
	if err != nil {
		return nil, fmt.Errorf("build URL: %w", err)
	}

	mp := &MetricsSnapshot{}

	if err := getJSON(url, &mp.data); err != nil {
		return nil, fmt.Errorf("get JSON: %w", err)
	}

	return mp, nil
}

func DiffStatusSensors(oldSnapshot, newSnapshot *MetricsSnapshot, typ, method, name, status string) (float64, error) {
	oldSensors := oldSnapshot.FindStatusSensors(typ, method, name, status)
	if len(oldSensors) != 1 {
		return 0, fmt.Errorf("unexpected number of sensors in old snapshot: %d", len(oldSensors))
	}

	newSensors := newSnapshot.FindStatusSensors(typ, method, name, status)
	if len(newSensors) != 1 {
		return 0, fmt.Errorf("unexpected number of sensors in old snapshot: %d", len(newSensors))
	}

	return newSensors[0].Value - oldSensors[0].Value, nil
}
