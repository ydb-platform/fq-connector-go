package opensearch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.DataSource[any] = (*dataSource)(nil)

type dataSource struct {
	retrierSet *retry.RetrierSet
	cfg        *config.TOpenSearchConfig
}

func NewDataSource(retrierSet *retry.RetrierSet, cfg *config.TOpenSearchConfig) datasource.DataSource[any] {
	return &dataSource{retrierSet: retrierSet, cfg: cfg}
}

func (ds *dataSource) DescribeTable(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	dsi := request.DataSourceInstance

	if dsi.Protocol != api_common.EGenericProtocol_HTTP {
		return nil, fmt.Errorf("cannot run OpenSearch connection with protocol '%v'", dsi.Protocol)
	}

	var client *opensearchapi.Client

	err := ds.retrierSet.MakeConnection.Run(ctx, logger,
		func() error {
			var err error
			client, err = ds.makeConnection(ctx, logger, dsi)

			return err
		},
	)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	indexName := request.Table
	res, err := client.Indices.Mapping.Get(
		ctx,
		&opensearchapi.MappingGetReq{Indices: []string{indexName}},
	)

	if err != nil {
		return nil, fmt.Errorf("get mapping failed: %w", err)
	}

	defer closeResponseBody(logger, res.Inspect().Response.Body)

	err = checkStatusCode(res.Inspect().Response.StatusCode)
	if err != nil {
		return nil, fmt.Errorf("check status code: %w", err)
	}

	var result map[string]any

	err = json.NewDecoder(res.Inspect().Response.Body).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("decode response body: %w", err)
	}

	mapping, ok := result[indexName].(map[string]any)["mappings"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("extract mappings: invalid response format")
	}

	columns, err := parseMapping(logger, mapping)
	if err != nil {
		return nil, fmt.Errorf("parse mapping: %w", err)
	}

	return &api_service_protos.TDescribeTableResponse{
		Schema: &api_service_protos.TSchema{Columns: columns},
	}, nil
}

func (*dataSource) ListSplits(
	ctx context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult) error {
	// By default we deny table splitting
	select {
	case resultChan <- &datasource.ListSplitResult{Slct: slct, Description: nil}:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (*dataSource) ReadSplit(
	_ context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TReadSplitsRequest,
	_ *api_service_protos.TSplit,
	_ paging.SinkFactory[any],
) error {
	return fmt.Errorf("unimplemented")
}

func (ds *dataSource) makeConnection(
	ctx context.Context,
	logger *zap.Logger,
	dsi *api_common.TGenericDataSourceInstance,
) (*opensearchapi.Client, error) {
	instanceAddress := fmt.Sprintf("%s://%s:%d", dsi.Protocol, dsi.Endpoint.Host, dsi.Endpoint.Port)
	logger.Debug("creating connection",
		zap.String("address", instanceAddress),
	)

	cfg := opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: []string{instanceAddress},
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: false,
				},
				DialContext: (&net.Dialer{
					Timeout: common.MustDurationFromString(ds.cfg.DialTimeout),
				}).DialContext,
				ResponseHeaderTimeout: common.MustDurationFromString(ds.cfg.ResponseHeaderTimeout),
			},
			Username: dsi.Credentials.GetBasic().Username,
			Password: dsi.Credentials.GetBasic().Password,
		},
	}

	client, err := opensearchapi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("client creation failed: %w", err)
	}

	logger.Debug("pinging connection")

	err = pingWithTimeout(ctx, logger, client, common.MustDurationFromString(ds.cfg.PingConnectionTimeout))
	if err != nil {
		return nil, fmt.Errorf("ping OpenSearch: %w", err)
	}

	logger.Info("successfully connected", zap.String("address", instanceAddress))

	return client, nil
}

func pingWithTimeout(
	ctx context.Context,
	logger *zap.Logger,
	client *opensearchapi.Client,
	timeout time.Duration,
) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pingReq := opensearchapi.PingReq{}

	res, err := client.Ping(ctxWithTimeout, &pingReq)
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	defer closeResponseBody(logger, res.Body)

	return checkStatusCode(res.StatusCode)
}

func closeResponseBody(
	logger *zap.Logger,
	body io.ReadCloser,
) {
	if body == nil {
		return
	}

	common.LogCloserError(logger, body, "close response body")
}

func checkStatusCode(statusCode int) error {
	if statusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", statusCode)
	}

	return nil
}
