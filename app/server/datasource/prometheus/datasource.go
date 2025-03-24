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
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

const (
	prometheusClientName = "fq-connector-remote-read-client"

	defaultConnectionTimeout = 10 * time.Second
)

type dataSource struct{}

var _ datasource.DataSource[any] = (*dataSource)(nil)

func NewDataSource() datasource.DataSource[any] {
	return &dataSource{}
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

	client, err := ds.makeConnection(dsi)
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
	logger *zap.Logger,
	request *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	//TODO implement me
	panic("implement me")
}

func (ds *dataSource) ReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sinkFactory paging.SinkFactory[any],
) error {
	//dsi := split.Select.DataSourceInstance
	//
	//if dsi.Protocol != api_common.EGenericProtocol_HTTP {
	//	return fmt.Errorf("cannot create Prometheus client using '%v' protocol", dsi.Protocol)
	//}
	//
	//// TODO: Get Prometheus options
	//
	//sinks, err := sinkFactory.MakeSinks([]*paging.SinkParams{{Logger: logger}})
	//if err != nil {
	//	return fmt.Errorf("make sinks: %w", err)
	//}
	//return ds.doReadSplitSingleConn()
	//TODO implement me
	panic("implement me")
}

func (ds *dataSource) makeConnection(dsi *api_common.TGenericDataSourceInstance) (remote.ReadClient, error) {
	readClient, err := remote.NewReadClient(prometheusClientName, &remote.ClientConfig{
		URL: &config.URL{URL: &url.URL{
			// TODO: Think about https
			Scheme: "http",
			Host:   common.EndpointToString(dsi.Endpoint),
			Path:   "/api/v1/read",
		}},
		// TODO: Get timeout from options
		Timeout:          model.Duration(defaultConnectionTimeout),
		HTTPClientConfig: config.HTTPClientConfig{
			// TODO: TLS config
			//TLSConfig:       config.TLSConfig{
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
			//},
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
	dsi *api_common.TGenericDataSourceInstance,
	_ *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
	client remote.ReadClient,
) error {
	//fromMatcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", split.Select.From.GetTable())
	//if err != nil {
	//	return fmt.Errorf("new matcher from: %w", err)
	//}
	//
	//// TODO: Limit && Offset (from && to params)
	//pbQuery, err := remote.ToQuery(
	//	0,
	//	int64(model.TimeFromUnixNano(time.Now().Add(time.Minute).UnixNano())),
	//	[]*labels.Matcher{fromMatcher},
	//	nil,
	//)
	//if err != nil {
	//	return fmt.Errorf("to query: %w", err)
	//}
	//
	return nil
}
