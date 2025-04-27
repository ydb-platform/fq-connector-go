package opensearch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/observation"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.DataSource[any] = (*dataSource)(nil)

type dataSource struct {
	retrierSet  *retry.RetrierSet
	cc          conversion.Collection
	cfg         *config.TOpenSearchConfig
	queryLogger common.QueryLogger
}

func NewDataSource(retrierSet *retry.RetrierSet, cfg *config.TOpenSearchConfig, cc conversion.Collection) datasource.DataSource[any] {
	return &dataSource{retrierSet: retrierSet, cfg: cfg, cc: cc}
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
		return nil, fmt.Errorf("get mapping: %w", err)
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

func (ds *dataSource) ReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	_ observation.IncomingQueryID,
	_ *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sinkFactory paging.SinkFactory[any],
) error {
	dsi := split.Select.DataSourceInstance

	if dsi.Protocol != api_common.EGenericProtocol_HTTP {
		return fmt.Errorf("cannot run OpenSearch connection with protocol '%v'", dsi.Protocol)
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
		return fmt.Errorf("make connection: %w", err)
	}

	if split.Select.From.Table == "" {
		return common.ErrEmptyTableName
	}

	ds.queryLogger.Dump(split.Select.From.Table, split.Select.What.String())

	sinks, err := sinkFactory.MakeSinks([]*paging.SinkParams{{Logger: logger}})
	if err != nil {
		return fmt.Errorf("make sinks: %w", err)
	}

	sink := sinks[0]

	if err := ds.doReadSplitSingleConn(ctx, logger, split, sink, client); err != nil {
		return fmt.Errorf("read split single conn: %w", err)
	}

	sink.Finish()

	return nil
}

func (ds *dataSource) doReadSplitSingleConn(
	ctx context.Context,
	logger *zap.Logger,
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
	client *opensearchapi.Client,
) error {
	searchResp, err := ds.initialSearch(
		ctx,
		logger,
		client,
		split,
		ds.cfg.BatchSize,
		common.MustDurationFromString(ds.cfg.ScrollTimeout),
	)
	if err != nil {
		return fmt.Errorf("initial search: %w", err)
	}

	if searchResp.ScrollID == nil {
		return fmt.Errorf("scroll id is nil")
	}

	reader, err := prepareDocumentReader(split, ds.cc)
	if err != nil {
		return fmt.Errorf("make document reader: %w", err)
	}

	scrollId := searchResp.ScrollID
	hits := searchResp.Hits

	for {
		if len(hits.Hits) == 0 {
			break
		}

		if err := processHitsBatch(logger, hits.Hits, reader, sink); err != nil {
			return fmt.Errorf("process hit: %w", err)
		}

		nextResp, err := ds.getNextScrollBatch(ctx, logger, client, *scrollId, common.MustDurationFromString(ds.cfg.ScrollTimeout))
		if err != nil {
			return fmt.Errorf("scroll: %w", err)
		}

		closeResponseBody(logger, nextResp.Inspect().Response.Body)
		hits = nextResp.Hits
	}

	return clearScroll(ctx, client, *scrollId)
}

func (ds *dataSource) initialSearch(
	ctx context.Context,
	logger *zap.Logger,
	client *opensearchapi.Client,
	split *api_service_protos.TSplit,
	batchSize uint64,
	scrollTimeout time.Duration,
) (*opensearchapi.SearchResp, error) {
	req := &opensearchapi.SearchReq{
		Indices: []string{split.Select.From.Table},
		Body: strings.NewReader(fmt.Sprintf(`{
                "size": %d,
                "query": {"match_all": {}}
            }`, batchSize)),
		Params: opensearchapi.SearchParams{
			Scroll: scrollTimeout,
		},
	}

	var (
		resp      *opensearchapi.SearchResp
		searchErr error
	)

	err := ds.retrierSet.Query.Run(
		ctx,
		logger,
		func() error {
			resp, searchErr = client.Search(ctx, req)
			if searchErr != nil {
				return fmt.Errorf("search: %w", searchErr)
			}

			return nil
		},
	)

	closeResponseBody(logger, resp.Inspect().Response.Body)

	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}

	return resp, err
}

func prepareDocumentReader(
	split *api_service_protos.TSplit,
	cc conversion.Collection,
) (*documentReader, error) {
	arrowSchema, err := common.SelectWhatToArrowSchema(split.Select.What)
	if err != nil {
		return nil, fmt.Errorf("select what to Arrow schema: %w", err)
	}

	ydbSchema, err := common.SelectWhatToYDBTypes(split.Select.What)
	if err != nil {
		return nil, fmt.Errorf("select what to YDB schema: %w", err)
	}

	return makeDocumentReader(arrowSchema, ydbSchema, cc)
}

func processHitsBatch(
	logger *zap.Logger,
	hits []opensearchapi.SearchHit,
	reader *documentReader,
	sink paging.Sink[any],
) error {
	for _, hit := range hits {
		if err := reader.accept(logger, hit); err != nil {
			return fmt.Errorf("accept document: %w", err)
		}

		if err := sink.AddRow(reader.transformer); err != nil {
			return fmt.Errorf("add row to sink: %w", err)
		}
	}

	return nil
}

func (ds *dataSource) getNextScrollBatch(
	ctx context.Context,
	logger *zap.Logger,
	client *opensearchapi.Client,
	scrollID string,
	scrollTimeout time.Duration,
) (*opensearchapi.ScrollGetResp, error) {
	var resp *opensearchapi.ScrollGetResp

	err := ds.retrierSet.Query.Run(ctx, logger, func() error {
		var err error
		resp, err = client.Scroll.Get(ctx, opensearchapi.ScrollGetReq{
			ScrollID: scrollID,
			Params: opensearchapi.ScrollGetParams{
				Scroll: scrollTimeout,
			},
		})

		return err
	})

	return resp, err
}

func clearScroll(
	ctx context.Context,
	client *opensearchapi.Client,
	scrollID string,
) error {
	if _, err := client.Scroll.Delete(ctx, opensearchapi.ScrollDeleteReq{
		ScrollIDs: []string{scrollID},
	}); err != nil {
		return fmt.Errorf("clear scroll: %w", err)
	}

	return nil
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
		return nil, fmt.Errorf("client creation: %w", err)
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
