package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	conf "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

const (
	prometheusClientName         = "fq-connector-remote-read-client"
	prometheusGetLabelsURLFormat = "%s/api/v1/labels?match[]=%s"
	prometheusGetLabelsTimeout   = 10 * time.Second

	httpSchema  = "http"
	httpsSchema = "https"
)

type CloseFunc func()

type ReadClient struct {
	promClient remote.ReadClient
	promURL    *url.URL
}

func NewReadClient(dsi *api_common.TGenericDataSourceInstance, cfg *config.TPrometheusConfig) (*ReadClient, error) {
	remoteReadURL := &conf.URL{URL: &url.URL{
		Scheme: chooseHTTPSchema(dsi.GetUseTls()),
		Host:   common.EndpointToString(dsi.Endpoint),
		Path:   "/api/v1/read",
	}}

	readClient, err := remote.NewReadClient(prometheusClientName, &remote.ClientConfig{
		URL:              remoteReadURL,
		Timeout:          model.Duration(common.MustDurationFromString(cfg.GetOpenConnectionTimeout())),
		ChunkedReadLimit: cfg.GetChunkedReadLimitBytes(),
	})
	if err != nil {
		return nil, fmt.Errorf("new Prometheus remote read client: %w", err)
	}

	return &ReadClient{
		promClient: readClient,
		promURL: &url.URL{
			Scheme: remoteReadURL.Scheme,
			Host:   remoteReadURL.Host,
		},
	}, nil
}

func (rc *ReadClient) Read(ctx context.Context, pbQuery *prompb.Query) (storage.SeriesSet, CloseFunc, error) {
	ctx, cancel := context.WithCancel(ctx)

	timeSeries, err := rc.promClient.Read(ctx, pbQuery, false)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("client remote read: %w", err)
	}

	return timeSeries, func() {
		cancel()
		// Because we can`t close body directly using Prometheus read client
		// `timeSeries.Next()` clear body and close it after context cancellation
		timeSeries.Next()
	}, nil
}

func (rc *ReadClient) Schema(ctx context.Context, metric string) ([]*Ydb.Column, error) {
	labels, err := rc.getAllLabels(ctx, metric)
	if err != nil {
		return nil, fmt.Errorf("get all metric labels: %w", err)
	}

	return metricToYdbSchema(labels), nil
}

type getLabelsResponse struct {
	Status string   `json:"status"`
	Labels []string `json:"data"`
}

func (rc *ReadClient) getAllLabels(ctx context.Context, metric string) ([]string, error) {
	getLabelsURL := fmt.Sprintf(prometheusGetLabelsURLFormat,
		rc.promURL.String(),
		url.QueryEscape(metric),
	)

	ctx, cancel := context.WithTimeout(ctx, prometheusGetLabelsTimeout)
	defer cancel()

	labelsRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, getLabelsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("new request with context: %w", err)
	}

	resp, err := http.DefaultClient.Do(labelsRequest)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read all body: %w", err)
	}

	var labelsResponse getLabelsResponse

	err = json.Unmarshal(body, &labelsResponse)
	if err != nil {
		return nil, fmt.Errorf("unmarshal JSON: %w", err)
	}

	if labelsResponse.Status != "success" {
		return nil, fmt.Errorf("non success status: %s", labelsResponse.Status)
	}

	return labelsResponse.Labels, nil
}

func chooseHTTPSchema(useTLS bool) string {
	if useTLS {
		return httpsSchema
	}

	return httpSchema
}
