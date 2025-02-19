package streaming

import (
	"fmt"

	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/common"
	"go.uber.org/zap"
)

type ListSplitsStreamer struct {
	stream       api_service.Connector_ListSplitsServer
	results      chan *datasource.ListSplitResult
	splitCounter int
	logger       *zap.Logger
}

func (s *ListSplitsStreamer) AddSplitResult(result *datasource.ListSplitResult) {
	select {
	case s.results <- result:
	case <-s.stream.Context().Done():
		return
	}
}

func (s *ListSplitsStreamer) Close() {
	close(s.results)
}

func (s *ListSplitsStreamer) Run() error {
	for {
		select {
		case result, ok := <-s.results:
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

func (s *ListSplitsStreamer) sendResultToStream(result *datasource.ListSplitResult) error {
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

func NewListSplitsStreamer(
	logger *zap.Logger,
	stream api_service.Connector_ListSplitsServer,
) *ListSplitsStreamer {
	return &ListSplitsStreamer{
		stream:  stream,
		results: make(chan *datasource.ListSplitResult, 32),
		logger:  logger,
	}
}
