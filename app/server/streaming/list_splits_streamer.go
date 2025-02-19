package streaming

import (
	"fmt"

	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
	"go.uber.org/zap"
)

type ListSplitsStreamer[T paging.Acceptor] struct {
	request      *api_service_protos.TListSplitsRequest
	slct         *api_service_protos.TSelect
	stream       api_service.Connector_ListSplitsServer
	dataSource   datasource.DataSource[T]
	splitCounter int
	logger       *zap.Logger
}

func (s *ListSplitsStreamer[T]) Run() error {
	results, err := s.dataSource.ListSplits(s.stream.Context(), s.logger, s.request, s.slct)
	if err != nil {
		return fmt.Errorf("list splits: %w", err)
	}

	for {
		select {
		case result, ok := <-results:
			if !ok {
				return nil
			}

			if err := s.sendResultToStream(result); err != nil {
				return fmt.Errorf("send result to stream: %w", err)
			}
		case <-s.stream.Context().Done():
			return s.stream.Context().Err()
		}
	}
}

func (s *ListSplitsStreamer[T]) sendResultToStream(result *datasource.ListSplitResult) error {
	if result.Error != nil {
		return fmt.Errorf("result error: %w", result.Error)
	}

	s.logger.Debug(
		"Got split",
		zap.Int("id", s.splitCounter),
		zap.Any("select", result.Slct),
		zap.ByteString("description", result.Description),
	)

	// For the sake of simplicity, we make a distinct message for each split.
	// TODO: consider split batching as they splits should be small in general.
	response := &api_service_protos.TListSplitsResponse{
		Error: common.NewSuccess(),
		Splits: []*api_service_protos.TSplit{
			{
				Select: result.Slct,
				Payload: &api_service_protos.TSplit_Description{
					Description: result.Description,
				},
			},
		},
	}

	if err := s.stream.Send(response); err != nil {
		return fmt.Errorf("stream send: %w", err)
	}

	return nil
}

func NewListSplitsStreamer[T paging.Acceptor](
	logger *zap.Logger,
	stream api_service.Connector_ListSplitsServer,
	dataSource datasource.DataSource[T],
	request *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
) *ListSplitsStreamer[T] {
	return &ListSplitsStreamer[T]{
		stream:     stream,
		dataSource: dataSource,
		logger:     logger,
		request:    request,
		slct:       slct,
	}
}
