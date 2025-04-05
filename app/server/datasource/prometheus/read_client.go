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

type CloseFunc func()

type ReadClient struct {
	promClient remote.ReadClient
}

func NewReadClient(dsi *api_common.TGenericDataSourceInstance, cfg *config.TPrometheusConfig) (*ReadClient, error) {
	options := getPrometheusOptions(dsi)

	readClient, err := remote.NewReadClient(prometheusClientName, &remote.ClientConfig{
		URL: &conf.URL{URL: &url.URL{
			Scheme: options.GetSchema().String(),
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

func getPrometheusOptions(dsi *api_common.TGenericDataSourceInstance) *api_common.TPrometheusDataSourceOptions {
	schema := api_common.TPrometheusDataSourceOptions_HTTP

	dsiOptions := dsi.GetPrometheusOptions()
	if dsiOptions != nil && dsiOptions.GetSchema() == api_common.TPrometheusDataSourceOptions_HTTPS {
		schema = dsiOptions.GetSchema()
	}

	return &api_common.TPrometheusDataSourceOptions{Schema: schema}
}
