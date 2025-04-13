package prometheus

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

type dataSource struct {
	retrierSet *retry.RetrierSet
	cfg        *config.TPrometheusConfig
	cc         conversion.Collection
}

var _ datasource.DataSource[any] = (*dataSource)(nil)

func NewDataSource(retrierSet *retry.RetrierSet, cfg *config.TPrometheusConfig, cc conversion.Collection) datasource.DataSource[any] {
	return &dataSource{
		retrierSet: retrierSet,
		cfg:        cfg,
		cc:         cc,
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

	logger.Debug("do read Prometheus schema request")

	var columns []*Ydb.Column

	err = ds.retrierSet.Query.Run(
		ctx,
		logger,
		func() error {
			var queryErr error

			columns, queryErr = client.Schema(ctx, request.GetTable())
			if queryErr != nil {
				return fmt.Errorf("get prometheus schema: %w", queryErr)
			}

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("retrier set query run: %w", err)
	}

	logger.Info("schema have been read successfully")

	return &api_service_protos.TDescribeTableResponse{Schema: &api_service_protos.TSchema{
		Columns: columns,
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

	return ds.doReadSplit(ctx, logger, request, split, sinks[0], client)
}

func (ds *dataSource) doReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
	client *ReadClient,
) error {
	promQLExpr, err := NewPromQLBuilder(logger).
		From(split.Select.From.GetTable()).
		WithYdbWhere(split.Select.GetWhere(), request.GetFiltering())
	if err != nil {
		return fmt.Errorf("build promql expression: %w", err)
	}

	pbQuery, err := promQLExpr.ToQuery()
	if err != nil {
		return fmt.Errorf("promql builder to query: %w", err)
	}

	logger.Debug("do remote read Prometheus request")

	var timeSeries storage.SeriesSet

	var seriesClose CloseFunc

	err = ds.retrierSet.Query.Run(
		ctx,
		logger,
		func() error {
			var queryErr error

			timeSeries, seriesClose, queryErr = client.Read(ctx, pbQuery)
			if queryErr != nil {
				return fmt.Errorf("client remote read: %w", queryErr)
			}

			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("retrier set query run: %w", err)
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
			if err = reader.accept(series.Labels(), ts, v); err != nil {
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
