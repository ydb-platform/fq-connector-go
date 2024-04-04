package common

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type MetricsProvider struct {
	data map[string]any
}

func (mp *MetricsProvider) Find(typ string, name string) (map[string]any, error) {
	metrics, ok := mp.data["metrics"].([]any)
	if !ok {
		panic("invalid response")
	}

	for _, item_ := range metrics {
		item, ok := item_.(map[string]any)
		if !ok {
			panic("invalid response")
		}

		labels, ok := item["labels"].(map[string]any)
		if !ok {
			panic("invalid response")
		}

		if item["type"] == typ && labels["name"] == name {
			return item, nil
		}
	}

	return nil, fmt.Errorf("not found")
}

func getJSON(client *http.Client, url string, target interface{}) error {
	r, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}

	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}

func NewMetricsProvider(url string) (*MetricsProvider, error) {
	client := &http.Client{Timeout: 10 * time.Second}

	mp := &MetricsProvider{}

	if err := getJSON(client, url, &mp.data); err != nil {
		return nil, fmt.Errorf("get JSON: %w", err)
	}

	return mp, nil
}
