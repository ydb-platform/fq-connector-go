package prometheus

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	cfg "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

const (
	prometheusClientName = "fq-connector-remote-read-client"

	defaultConnectionTimeout = 10 * time.Second
)

type dataSource struct {
	cc conversion.Collection
}

var _ datasource.DataSource[any] = (*dataSource)(nil)

func NewDataSource(cc conversion.Collection) datasource.DataSource[any] {
	return &dataSource{
		cc: cc,
	}
}

func (ds *dataSource) DescribeTable(
	ctx context.Context,
	_ *zap.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	dsi := request.DataSourceInstance

	if dsi.Protocol != api_common.EGenericProtocol_HTTP {
		return nil, fmt.Errorf("cannot create Prometheus client using '%v' protocol", dsi.Protocol)
	}

	// TODO: Get Prometheus options

	client, err := makeConnection(dsi)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	fromMatcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", request.GetTable())
	if err != nil {
		return nil, fmt.Errorf("new matcher from: %w", err)
	}

	pbQuery, err := remote.ToQuery(
		0,
		int64(model.TimeFromUnixNano(time.Now().UnixNano())),
		[]*labels.Matcher{fromMatcher},
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("to prompb query: %w", err)
	}

	timeSeries, err := client.Read(ctx, pbQuery, false)
	if err != nil {
		return nil, fmt.Errorf("client remote read: %w", err)
	}

	if !timeSeries.Next() {
		return nil, fmt.Errorf("time series next: %w", ErrEmptyTimeSeries)
	}

	return &api_service_protos.TDescribeTableResponse{Schema: &api_service_protos.TSchema{
		Columns: timeSeriesToYdbSchema(timeSeries.At().Labels()),
	}}, nil
}

func (ds *dataSource) ListSplits(
	ctx context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	// By default, we deny table splitting
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
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sinkFactory paging.SinkFactory[any],
) error {
	dsi := split.Select.DataSourceInstance

	if dsi.Protocol != api_common.EGenericProtocol_HTTP {
		return fmt.Errorf("cannot create Prometheus client using '%v' protocol", dsi.Protocol)
	}

	// TODO: Get Prometheus options

	client, err := makeConnection(dsi)
	if err != nil {
		return fmt.Errorf("make connection: %w", err)
	}

	sinks, err := sinkFactory.MakeSinks([]*paging.SinkParams{{Logger: logger}})
	if err != nil {
		return fmt.Errorf("make sinks: %w", err)
	}

	return ds.doReadSplitSingleConn(ctx, logger, request, split, sinks[0], client)
}

func makeConnection(dsi *api_common.TGenericDataSourceInstance) (remote.ReadClient, error) {
	readClient, err := remote.NewReadClient(prometheusClientName, &remote.ClientConfig{
		URL: &config.URL{URL: &url.URL{
			// TODO: Think about https
			Scheme: "http",
			Host:   common.EndpointToString(dsi.Endpoint),
			Path:   "/api/v1/read",
		}},
		// TODO: Get timeout from options
		Timeout: model.Duration(defaultConnectionTimeout),
		HTTPClientConfig: config.HTTPClientConfig{
			// TODO: TLS config
			// TLSConfig:       config.TLSConfig{
			//	CA:                 "",
			//	Cert:               "",
			//	Key:                "",
			//	CAFile:             "",
			//	CertFile:           "",
			//	KeyFile:            "",
			//	CARef:              "",
			//	CertRef:            "",
			//	KeyRef:             "",
			//	ServerName:         "",
			//	InsecureSkipVerify: false,
			//	MinVersion:         0,
			//	MaxVersion:         0,
			// },
		},
		// TODO: Get limit from options
		ChunkedReadLimit: cfg.DefaultChunkedReadLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("new Prometheus remote read client: %w", err)
	}

	return readClient, nil
}

func (ds *dataSource) doReadSplitSingleConn(
	ctx context.Context,
	logger *zap.Logger,
	_ *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
	client remote.ReadClient,
) error {
	fromMatcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", split.Select.From.GetTable())
	if err != nil {
		return fmt.Errorf("new matcher from: %w", err)
	}

	// TODO: Limit && Offset (<from> and <to> params)
	// Now we read all metrics
	pbQuery, err := remote.ToQuery(
		// 0,
		// int64(model.TimeFromUnixNano(time.Now().Add(time.Minute).UnixNano())),
		int64(model.TimeFromUnixNano(time.Now().Add(-time.Minute).UnixNano())),
		int64(model.TimeFromUnixNano(time.Now().UnixNano())),
		[]*labels.Matcher{fromMatcher},
		nil,
	)
	if err != nil {
		return fmt.Errorf("to query: %w", err)
	}

	timeSeries, err := client.Read(ctx, pbQuery, false)
	if err != nil {
		return fmt.Errorf("client remote read: %w", err)
	}

	arrowSchema, err := common.SelectWhatToArrowSchema(split.Select.What)
	if err != nil {
		return fmt.Errorf("select what to Arrow schema: %w", err)
	}

	ydbSchema, err := common.SelectWhatToYDBTypes(split.Select.What)
	if err != nil {
		return fmt.Errorf("select what to YDB schema: %w", err)
	}

	reader, err := makeMetricsReader(arrowSchema, ydbSchema, ds.cc)
	if err != nil {
		return fmt.Errorf("make metrics reader: %w", err)
	}

	var it chunkenc.Iterator

	for timeSeries.Next() {
		series := timeSeries.At()
		iter := series.Iterator(it)

		for vt := iter.Next(); vt != chunkenc.ValNone; vt = iter.Next() {
			if vt != chunkenc.ValFloat {
				return fmt.Errorf("series value must be %T", chunkenc.ValFloat)
			}

			ts, v := iter.At()
			if err = reader.accept(logger, series.Labels(), ts, v); err != nil {
				return fmt.Errorf("accept time series: %w", err)
			}

			if err = sink.AddRow(reader.transformer); err != nil {
				return fmt.Errorf("add row to sink: %w", err)
			}
		}

		if err := timeSeries.Err(); err != nil {
			return fmt.Errorf("time series error: %w", err)
		}
	}

	sink.Finish()

	return nil
}
