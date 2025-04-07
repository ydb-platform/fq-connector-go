package prometheus

import (
	"context"
	"fmt"
	"net/url"

	conf "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

const (
	prometheusClientName = "fq-connector-remote-read-client"

	httpSchema  = "http"
	httpsSchema = "https"
)

type CloseFunc func()

type ReadClient struct {
	promClient remote.ReadClient
}

func NewReadClient(dsi *api_common.TGenericDataSourceInstance, cfg *config.TPrometheusConfig) (*ReadClient, error) {
	readClient, err := remote.NewReadClient(prometheusClientName, &remote.ClientConfig{
		URL: &conf.URL{URL: &url.URL{
			Scheme: chooseSchema(dsi.GetUseTls()),
			Host:   common.EndpointToString(dsi.Endpoint),
			Path:   "/api/v1/read",
		}},
		Timeout: model.Duration(common.MustDurationFromString(cfg.GetOpenConnectionTimeout())),
		// TODO: Check
		HTTPClientConfig: conf.HTTPClientConfig{
			TLSConfig: conf.TLSConfig{InsecureSkipVerify: dsi.GetUseTls()},
		},
		ChunkedReadLimit: cfg.GetChunkedReadLimit(),
	})
	if err != nil {
		return nil, fmt.Errorf("new Prometheus remote read client: %w", err)
	}

	return &ReadClient{promClient: readClient}, nil
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

func chooseSchema(useTLS bool) string {
	if useTLS {
		return httpsSchema
	}

	return httpSchema
}
