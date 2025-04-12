package opensearch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"go.uber.org/zap"
	"io"
	"net"
	"net/http"
	"time"
)

var _ datasource.DataSource[any] = (*dataSource)(nil)

type dataSource struct {
	retrierSet *retry.RetrierSet
}

func NewDataSource(retrierSet *retry.RetrierSet) datasource.DataSource[any] {
	return &dataSource{retrierSet: retrierSet}
}

func (ds *dataSource) DescribeTable(ctx context.Context, logger *zap.Logger, request *api_service_protos.TDescribeTableRequest) (*api_service_protos.TDescribeTableResponse, error) {
	dsi := request.DataSourceInstance

	if dsi.Protocol != api_common.EGenericProtocol_HTTP {
		return nil, fmt.Errorf("cannot run OpenSearch connection with protocol '%v'", dsi.Protocol)
	}

	var client *opensearch.Client

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
	res, err := client.Indices.GetMapping(
		client.Indices.GetMapping.WithContext(ctx),
		client.Indices.GetMapping.WithIndex(indexName),
	)
	if err != nil {
		return nil, fmt.Errorf("get mapping failed: %w", err)
	}
	defer closeResponseBody(res.Body)

	if err := checkStatusCode(res.StatusCode); err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	mapping, ok := result[indexName].(map[string]interface{})["mappings"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to extract mappings from response")
	}

	columns, err := parseMapping(logger, mapping)
	if err != nil {
		return nil, fmt.Errorf("parseMapping: %w", err)
	}

	return &api_service_protos.TDescribeTableResponse{
		Schema: &api_service_protos.TSchema{Columns: columns},
	}, nil
}

func (ds *dataSource) ListSplits(ctx context.Context, logger *zap.Logger, request *api_service_protos.TListSplitsRequest, slct *api_service_protos.TSelect, resultChan chan<- *datasource.ListSplitResult) error {
	//TODO implement me
	return fmt.Errorf("unimplemented")
}

func (ds *dataSource) ReadSplit(ctx context.Context, logger *zap.Logger, request *api_service_protos.TReadSplitsRequest, split *api_service_protos.TSplit, sinkFactory paging.SinkFactory[any]) error {
	//TODO implement me
	return fmt.Errorf("unimplemented")
}

func (ds *dataSource) makeConnection(
	ctx context.Context,
	logger *zap.Logger,
	dsi *api_common.TGenericDataSourceInstance,
) (*opensearch.Client, error) {
	if dsi == nil || dsi.Endpoint == nil || dsi.Endpoint.Host == "" {
		return nil, fmt.Errorf("invalid data source instance: missing endpoint or host")
	}

	cfg := opensearch.Config{
		Addresses: []string{
			fmt.Sprintf("%s://%s:%d", dsi.Protocol, dsi.Endpoint.Host, dsi.Endpoint.Port),
		},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: dsi.UseTls,
			},
			DialContext: (&net.Dialer{
				Timeout: 5 * time.Second, //TODO in config
			}).DialContext,
			ResponseHeaderTimeout: 10 * time.Second, //TODO in config
		},
		Username: dsi.Credentials.GetBasic().Username,
		Password: dsi.Credentials.GetBasic().Password,
	}

	client, err := opensearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	err = pingWithTimeout(ctx, client, 5*time.Second) //TODO in config
	if err != nil {
		return nil, fmt.Errorf("failed to ping OpenSearch: %w", err)
	}

	logger.Info("Successfully connected to OpenSearch")
	return client, nil
}

func pingWithTimeout(ctx context.Context, client *opensearch.Client, timeout time.Duration) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pingReq := opensearchapi.PingRequest{}

	res, err := pingReq.Do(ctxWithTimeout, client)
	if err != nil {
		return fmt.Errorf("failed to ping OpenSearch: %w", err)
	}
	defer closeResponseBody(res.Body)

	if err := checkStatusCode(res.StatusCode); err != nil {
		return err
	}

	return nil
}

func closeResponseBody(body io.ReadCloser) {
	if body == nil {
		return
	}
	if err := body.Close(); err != nil {
		fmt.Printf("failed to close response body: %v\n", err)
	}
}

func checkStatusCode(statusCode int) error {
	if statusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code from OpenSearch: %d", statusCode)
	}
	return nil
}
