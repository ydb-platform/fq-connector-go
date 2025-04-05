package prometheus

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

const (
	prometheusClientName = "fq-connector-remote-read-client"
	prometheusNameLabel  = "__name__"
)

type dataSource struct {
	cfg *config.TPrometheusConfig
	cc  conversion.Collection
}

var _ datasource.DataSource[any] = (*dataSource)(nil)

func NewDataSource(cfg *config.TPrometheusConfig, cc conversion.Collection) datasource.DataSource[any] {
	return &dataSource{
		cfg: cfg,
		cc:  cc,
	}
}

func (ds *dataSource) DescribeTable(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	dsi := request.DataSourceInstance

	if dsi.Protocol != api_common.EGenericProtocol_HTTP {
		return nil, fmt.Errorf("cannot create Prometheus client using '%v' protocol", dsi.Protocol)
	}

	client, err := NewReadClient(dsi, ds.cfg)
	if err != nil {
		return nil, fmt.Errorf("new read client: %w", err)
	}

	// To get the values of a specific metric, you must first create a PromQL query using the internal label `__name__`.
	// Comparison of the received PromQL query with SQL (metric name in Prometheus ~ table name in SQL):
	//
	// PromQL - `{__name__='some_metric'}`
	//
	// SQL - `SELECT * FROM some_metric`
	//
	// For more info: https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors
	fromMatcher, err := labels.NewMatcher(labels.MatchEqual, prometheusNameLabel, request.GetTable())
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

	logger.Debug("do remote read Prometheus request")

	timeSeries, seriesClose, err := client.Read(ctx, pbQuery)
	if err != nil {
		return nil, fmt.Errorf("client remote read: %w", err)
	}
	defer seriesClose()

	logger.Info("metrics have been read successfully")

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

	client, err := NewReadClient(dsi, ds.cfg)
	if err != nil {
		return fmt.Errorf("new read client: %w", err)
	}

	sinks, err := sinkFactory.MakeSinks([]*paging.SinkParams{{Logger: logger}})
	if err != nil {
		return fmt.Errorf("make sinks: %w", err)
	}

	return ds.doReadSplitSingleConn(ctx, logger, request, split, sinks[0], client)
}

func (ds *dataSource) doReadSplitSingleConn(
	ctx context.Context,
	logger *zap.Logger,
	_ *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
	client *ReadClient,
) error {
	fromMatcher, err := labels.NewMatcher(labels.MatchEqual, prometheusNameLabel, split.Select.From.GetTable())
	if err != nil {
		return fmt.Errorf("new matcher from: %w", err)
	}

	// TODO: Limit && Offset (<from> and <to> params)
	// Now we read all metrics
	pbQuery, err := remote.ToQuery(
		0,
		int64(model.TimeFromUnixNano(time.Now().Add(time.Minute).UnixNano())),
		[]*labels.Matcher{fromMatcher},
		nil,
	)
	if err != nil {
		return fmt.Errorf("to query: %w", err)
	}

	logger.Debug("do remote read Prometheus request")

	timeSeries, seriesClose, err := client.Read(ctx, pbQuery)
	if err != nil {
		return fmt.Errorf("client remote read: %w", err)
	}
	defer seriesClose()

	logger.Info("metrics have been read successfully")

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
