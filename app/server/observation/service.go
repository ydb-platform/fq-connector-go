package observation

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"

	observation "github.com/ydb-platform/fq-connector-go/api/observation"
)

// serviceImpl represents the gRPC service implementation
type serviceImpl struct {
	observation.UnimplementedObservationServiceServer
	storage  Storage
	logger   *zap.Logger
	server   *grpc.Server
	listener net.Listener
}

// Start starts the gRPC server
func (s *serviceImpl) Start() error {
	s.logger.Info("starting GRPC server", zap.String("address", s.listener.Addr().String()))

	if err := s.server.Serve(s.listener); err != nil {
		return fmt.Errorf("listener serve: %w", err)
	}

	return nil
}

// Stop stops the gRPC server
func (s *serviceImpl) Stop() {
	s.logger.Info("Shutting down gRPC service")
	s.server.GracefulStop()

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Error("Error closing listener", zap.Error(err))
		}
	}

	err := s.storage.Close()
	if err != nil {
		s.logger.Error("Error closing storage", zap.Error(err))
	} else {
		s.logger.Info("Storage closed successfully")
	}
}

// convertQueryState converts the internal QueryState to the proto QueryState
func convertQueryState(state QueryState) observation.QueryState {
	switch state {
	case QueryStateRunning:
		return observation.QueryState_QUERY_STATE_RUNNING
	case QueryStateFinished:
		return observation.QueryState_QUERY_STATE_FINISHED
	case QueryStateCancelled:
		return observation.QueryState_QUERY_STATE_CANCELLED
	default:
		return observation.QueryState_QUERY_STATE_UNSPECIFIED
	}
}

// convertProtoQueryState converts the proto QueryState to the internal QueryState
func convertProtoQueryState(state observation.QueryState) *QueryState {
	var result QueryState
	switch state {
	case observation.QueryState_QUERY_STATE_RUNNING:
		result = QueryStateRunning
		return &result
	case observation.QueryState_QUERY_STATE_FINISHED:
		result = QueryStateFinished
		return &result
	case observation.QueryState_QUERY_STATE_CANCELLED:
		result = QueryStateCancelled
		return &result
	case observation.QueryState_QUERY_STATE_UNSPECIFIED:
		return nil
	default:
		return nil
	}
}

// convertIncomingQuery converts an internal IncomingQuery to a proto IncomingQuery
func convertIncomingQuery(q *IncomingQuery) *observation.IncomingQuery {
	result := &observation.IncomingQuery{
		Id:             uint64(q.ID),
		DataSourceKind: q.DataSourceKind,
		RowsRead:       q.RowsRead,
		BytesRead:      q.BytesRead,
		State:          convertQueryState(q.State),
		CreatedAt:      timestamppb.New(q.CreatedAt),
		Error:          q.Error,
	}

	if q.FinishedAt != nil {
		result.FinishedAt = timestamppb.New(*q.FinishedAt)
	}

	return result
}

// convertOutgoingQuery converts an internal OutgoingQuery to a proto OutgoingQuery
func convertOutgoingQuery(q *OutgoingQuery) *observation.OutgoingQuery {
	result := &observation.OutgoingQuery{
		Id:               uint64(q.ID),
		IncomingQueryId:  uint64(q.IncomingQueryID),
		DatabaseName:     q.DatabaseName,
		DatabaseEndpoint: q.DatabaseEndpoint,
		QueryText:        q.QueryText,
		QueryArgs:        q.QueryArgs,
		State:            convertQueryState(q.State),
		CreatedAt:        timestamppb.New(q.CreatedAt),
		RowsRead:         q.RowsRead,
		Error:            q.Error,
	}

	if q.FinishedAt != nil {
		result.FinishedAt = timestamppb.New(*q.FinishedAt)
	}

	return result
}

// ListIncomingQueries implements the gRPC method to list incoming queries
func (s *serviceImpl) ListIncomingQueries(
	ctx context.Context,
	req *observation.ListIncomingQueriesRequest,
) (*observation.ListIncomingQueriesResponse, error) {
	logger := s.logger.With(
		zap.Int32("limit", req.Limit),
		zap.Int32("offset", req.Offset),
		zap.String("state", req.State.String()),
	)
	logger.Info("ListIncomingQueries request handling started")

	stateParam := convertProtoQueryState(req.State)

	// Set default limit if not specified
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 1000 // Use a reasonable default
	}

	queries, err := s.storage.ListIncomingQueries(stateParam, limit, int(req.Offset))
	if err != nil {
		logger.Error("Failed to list incoming queries", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to list incoming queries: %v", err)
	}

	protoQueries := make([]*observation.IncomingQuery, len(queries))
	for i, q := range queries {
		protoQueries[i] = convertIncomingQuery(q)
	}

	response := &observation.ListIncomingQueriesResponse{
		Queries:    protoQueries,
		TotalCount: int32(len(queries)),
	}

	logger.Info("ListIncomingQueries request handling finished", zap.Int("queries_count", len(queries)))
	return response, nil
}

// ListOutgoingQueries implements the gRPC method to list outgoing queries
func (s *serviceImpl) ListOutgoingQueries(
	ctx context.Context,
	req *observation.ListOutgoingQueriesRequest,
) (*observation.ListOutgoingQueriesResponse, error) {
	logger := s.logger.With(
		zap.Uint64("incoming_query_id", req.IncomingQueryId),
		zap.Int32("limit", req.Limit),
		zap.Int32("offset", req.Offset),
		zap.String("state", req.State.String()),
	)
	logger.Info("ListOutgoingQueries request handling started")

	var incomingQueryIDParam *IncomingQueryID
	if req.IncomingQueryId != 0 {
		id := IncomingQueryID(req.IncomingQueryId)
		incomingQueryIDParam = &id
	}

	stateParam := convertProtoQueryState(req.State)

	// Set default limit if not specified
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 1000 // Use a reasonable default
	}

	queries, err := s.storage.ListOutgoingQueries(incomingQueryIDParam, stateParam, limit, int(req.Offset))
	if err != nil {
		logger.Error("Failed to list outgoing queries", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to list outgoing queries: %v", err)
	}

	protoQueries := make([]*observation.OutgoingQuery, len(queries))
	for i, q := range queries {
		protoQueries[i] = convertOutgoingQuery(q)
	}

	response := &observation.ListOutgoingQueriesResponse{
		Queries:    protoQueries,
		TotalCount: int32(len(queries)),
	}

	logger.Info("ListOutgoingQueries request handling finished", zap.Int("queries_count", len(queries)))
	return response, nil
}

// ListRunningIncomingQueries implements the gRPC method to list running incoming queries
func (s *serviceImpl) ListRunningIncomingQueries(
	ctx context.Context,
	req *observation.ListRunningIncomingQueriesRequest,
) (*observation.ListIncomingQueriesResponse, error) {
	logger := s.logger
	logger.Info("ListRunningIncomingQueries request handling started")

	state := QueryStateRunning
	queries, err := s.storage.ListIncomingQueries(&state, 1000, 0)
	if err != nil {
		logger.Error("Failed to list running incoming queries", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to list running incoming queries: %v", err)
	}

	protoQueries := make([]*observation.IncomingQuery, len(queries))
	for i, q := range queries {
		protoQueries[i] = convertIncomingQuery(q)
	}

	response := &observation.ListIncomingQueriesResponse{
		Queries:    protoQueries,
		TotalCount: int32(len(queries)),
	}

	logger.Info("ListRunningIncomingQueries request handling finished", zap.Int("queries_count", len(queries)))
	return response, nil
}

// ListRunningOutgoingQueries implements the gRPC method to list running outgoing queries
func (s *serviceImpl) ListRunningOutgoingQueries(
	ctx context.Context,
	req *observation.ListRunningOutgoingQueriesRequest,
) (*observation.ListOutgoingQueriesResponse, error) {
	logger := s.logger
	logger.Info("ListRunningOutgoingQueries request handling started")

	state := QueryStateRunning
	queries, err := s.storage.ListOutgoingQueries(nil, &state, 1000, 0)
	if err != nil {
		logger.Error("Failed to list running outgoing queries", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to list running outgoing queries: %v", err)
	}

	protoQueries := make([]*observation.OutgoingQuery, len(queries))
	for i, q := range queries {
		protoQueries[i] = convertOutgoingQuery(q)
	}

	response := &observation.ListOutgoingQueriesResponse{
		Queries:    protoQueries,
		TotalCount: int32(len(queries)),
	}

	logger.Info("ListRunningOutgoingQueries request handling finished", zap.Int("queries_count", len(queries)))
	return response, nil
}

// ListSimilarOutgoingQueriesWithDifferentStats implements the gRPC method to find similar outgoing queries with different stats
func (s *serviceImpl) ListSimilarOutgoingQueriesWithDifferentStats(
	ctx context.Context,
	req *observation.ListSimilarOutgoingQueriesWithDifferentStatsRequest,
) (*observation.ListSimilarOutgoingQueriesWithDifferentStatsResponse, error) {
	s.logger.Info("ListSimilarOutgoingQueriesWithDifferentStats request handling started")

	// This method is deprecated and will be removed in the future
	return &observation.ListSimilarOutgoingQueriesWithDifferentStatsResponse{}, nil
}

// NewService creates a new gRPC observation service instance.
func NewService(
	logger *zap.Logger,
	cfg *config.TObservationConfig,
	storage Storage,
) (utils.Service, error) {
	// Create a listener
	addr := common.EndpointToString(cfg.Server.GetEndpoint())
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("net listen: %w", err)
	}

	// Create a new gRPC server
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)

	service := &serviceImpl{
		storage:  storage,
		logger:   logger,
		server:   grpcServer,
		listener: listener,
	}

	// Register the service with the gRPC server
	observation.RegisterObservationServiceServer(grpcServer, service)

	return service, nil
}
