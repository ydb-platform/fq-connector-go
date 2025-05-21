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
	// This import will be valid once the proto files are generated
	// observation "github.com/ydb-platform/fq-connector-go/api/observation/v1"
)

// Note: This file contains a gRPC service implementation for the observation service.
// Before using this code, you need to generate Go code from the proto file using protoc.
// The import above is commented out because the generated code doesn't exist yet.

// grpcServiceImpl represents the gRPC service implementation
type grpcServiceImpl struct {
	// This would be the generated service interface
	// observation.UnimplementedObservationServiceServer
	storage  Storage
	logger   *zap.Logger
	server   *grpc.Server
	listener net.Listener
}

// Start starts the gRPC server
func (s *grpcServiceImpl) Start() error {
	s.logger.Info("starting GRPC server", zap.String("address", s.listener.Addr().String()))

	if err := s.server.Serve(s.listener); err != nil {
		return fmt.Errorf("listener serve: %w", err)
	}

	return nil
}

// Stop stops the gRPC server
func (s *grpcServiceImpl) Stop() {
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
func convertQueryState(state QueryState) int32 {
	switch state {
	case QueryStateRunning:
		return 1 // QUERY_STATE_RUNNING
	case QueryStateFinished:
		return 2 // QUERY_STATE_FINISHED
	case QueryStateCancelled:
		return 3 // QUERY_STATE_CANCELLED
	default:
		return 0 // QUERY_STATE_UNSPECIFIED
	}
}

// convertIncomingQuery converts an internal IncomingQuery to a proto IncomingQuery
func convertIncomingQuery(q *IncomingQuery) *ProtoIncomingQuery {
	result := &ProtoIncomingQuery{
		Id:             uint64(q.ID),
		DataSourceKind: q.DataSourceKind,
		RowsRead:       q.RowsRead,
		BytesRead:      q.BytesRead,
		State:          int32(convertQueryState(q.State)),
		CreatedAt:      timestamppb.New(q.CreatedAt),
		Error:          q.Error,
	}

	if q.FinishedAt != nil {
		result.FinishedAt = timestamppb.New(*q.FinishedAt)
	}

	return result
}

// convertOutgoingQuery converts an internal OutgoingQuery to a proto OutgoingQuery
func convertOutgoingQuery(q *OutgoingQuery) *ProtoOutgoingQuery {
	result := &ProtoOutgoingQuery{
		Id:               uint64(q.ID),
		IncomingQueryId:  uint64(q.IncomingQueryID),
		DatabaseName:     q.DatabaseName,
		DatabaseEndpoint: q.DatabaseEndpoint,
		QueryText:        q.QueryText,
		QueryArgs:        q.QueryArgs,
		State:            int32(convertQueryState(q.State)),
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
func (s *grpcServiceImpl) ListIncomingQueries(
	ctx context.Context,
	req *ListIncomingQueriesRequest,
) (*ListIncomingQueriesResponse, error) {
	var stateParam *QueryState
	if req.State != nil {
		state := QueryState("")
		switch *req.State {
		case 1:
			state = QueryStateRunning
		case 2:
			state = QueryStateFinished
		case 3:
			state = QueryStateCancelled
		default:
			return nil, status.Error(codes.InvalidArgument, "invalid state parameter")
		}
		stateParam = &state
	}

	queries, err := s.storage.ListIncomingQueries(stateParam, int(req.Limit), int(req.Offset))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list incoming queries: %v", err)
	}

	protoQueries := make([]*ProtoIncomingQuery, len(queries))
	for i, q := range queries {
		protoQueries[i] = convertIncomingQuery(q)
	}

	return &ListIncomingQueriesResponse{
		Queries:    protoQueries,
		TotalCount: int32(len(queries)),
	}, nil
}

// ListRunningIncomingQueries implements the gRPC method to list running incoming queries
func (s *grpcServiceImpl) ListRunningIncomingQueries(
	ctx context.Context,
	req *ListRunningIncomingQueriesRequest,
) (*ListIncomingQueriesResponse, error) {
	state := QueryStateRunning
	queries, err := s.storage.ListIncomingQueries(&state, 1000, 0)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list running incoming queries: %v", err)
	}

	protoQueries := make([]*ProtoIncomingQuery, len(queries))
	for i, q := range queries {
		protoQueries[i] = convertIncomingQuery(q)
	}

	return &ListIncomingQueriesResponse{
		Queries:    protoQueries,
		TotalCount: int32(len(queries)),
	}, nil
}

// ListOutgoingQueries implements the gRPC method to list outgoing queries
func (s *grpcServiceImpl) ListOutgoingQueries(
	ctx context.Context,
	req *ListOutgoingQueriesRequest,
) (*ListOutgoingQueriesResponse, error) {
	var incomingQueryIDParam *IncomingQueryID
	if req.IncomingQueryId != nil {
		id := IncomingQueryID(*req.IncomingQueryId)
		incomingQueryIDParam = &id
	}

	var stateParam *QueryState
	if req.State != nil {
		state := QueryState("")
		switch *req.State {
		case 1:
			state = QueryStateRunning
		case 2:
			state = QueryStateFinished
		case 3:
			state = QueryStateCancelled
		default:
			return nil, status.Error(codes.InvalidArgument, "invalid state parameter")
		}
		stateParam = &state
	}

	queries, err := s.storage.ListOutgoingQueries(incomingQueryIDParam, stateParam, int(req.Limit), int(req.Offset))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list outgoing queries: %v", err)
	}

	protoQueries := make([]*ProtoOutgoingQuery, len(queries))
	for i, q := range queries {
		protoQueries[i] = convertOutgoingQuery(q)
	}

	return &ListOutgoingQueriesResponse{
		Queries:    protoQueries,
		TotalCount: int32(len(queries)),
	}, nil
}

// ListRunningOutgoingQueries implements the gRPC method to list running outgoing queries
func (s *grpcServiceImpl) ListRunningOutgoingQueries(
	ctx context.Context,
	req *ListRunningOutgoingQueriesRequest,
) (*ListOutgoingQueriesResponse, error) {
	state := QueryStateRunning
	queries, err := s.storage.ListOutgoingQueries(nil, &state, 1000, 0)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list running outgoing queries: %v", err)
	}

	protoQueries := make([]*ProtoOutgoingQuery, len(queries))
	for i, q := range queries {
		protoQueries[i] = convertOutgoingQuery(q)
	}

	return &ListOutgoingQueriesResponse{
		Queries:    protoQueries,
		TotalCount: int32(len(queries)),
	}, nil
}

// ListSimilarOutgoingQueriesWithDifferentStats implements the gRPC method to find similar outgoing queries with different stats
func (s *grpcServiceImpl) ListSimilarOutgoingQueriesWithDifferentStats(
	ctx context.Context,
	req *ListSimilarOutgoingQueriesWithDifferentStatsRequest,
) (*ListSimilarOutgoingQueriesWithDifferentStatsResponse, error) {
	similarQueryGroups, err := s.storage.ListSimilarOutgoingQueriesWithDifferentStats(s.logger)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to find similar outgoing queries with different stats: %v", err)
	}

	protoQueryGroups := make([]*QueryGroup, len(similarQueryGroups))
	for i, group := range similarQueryGroups {
		protoQueries := make([]*ProtoOutgoingQuery, len(group))
		for j, q := range group {
			protoQueries[j] = convertOutgoingQuery(q)
		}
		protoQueryGroups[i] = &QueryGroup{
			Queries: protoQueries,
		}
	}

	return &ListSimilarOutgoingQueriesWithDifferentStatsResponse{
		QueryGroups: protoQueryGroups,
	}, nil
}

// NewGRPCService creates a new gRPC observation service instance.
func NewGRPCService(
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

	service := &grpcServiceImpl{
		storage:  storage,
		logger:   logger,
		server:   grpcServer,
		listener: listener,
	}

	// Register the service with the gRPC server
	// This would be done once the proto files are generated
	// observation.RegisterObservationServiceServer(grpcServer, service)

	return service, nil
}

// These are placeholder types that would be replaced by the generated types
type (
	ProtoIncomingQuery struct {
		Id             uint64
		DataSourceKind string
		RowsRead       int64
		BytesRead      int64
		State          int32
		CreatedAt      *timestamppb.Timestamp
		FinishedAt     *timestamppb.Timestamp
		Error          string
	}

	ProtoOutgoingQuery struct {
		Id               uint64
		IncomingQueryId  uint64
		DatabaseName     string
		DatabaseEndpoint string
		QueryText        string
		QueryArgs        string
		State            int32
		CreatedAt        *timestamppb.Timestamp
		FinishedAt       *timestamppb.Timestamp
		RowsRead         int64
		Error            string
	}

	ListIncomingQueriesRequest struct {
		State  *int32
		Limit  int32
		Offset int32
	}

	ListIncomingQueriesResponse struct {
		Queries    []*ProtoIncomingQuery
		TotalCount int32
	}

	ListRunningIncomingQueriesRequest struct{}

	ListOutgoingQueriesRequest struct {
		IncomingQueryId *uint64
		State           *int32
		Limit           int32
		Offset          int32
	}

	ListOutgoingQueriesResponse struct {
		Queries    []*ProtoOutgoingQuery
		TotalCount int32
	}

	ListRunningOutgoingQueriesRequest struct{}

	ListSimilarOutgoingQueriesWithDifferentStatsRequest struct{}

	QueryGroup struct {
		Queries []*ProtoOutgoingQuery
	}

	ListSimilarOutgoingQueriesWithDifferentStatsResponse struct {
		QueryGroups []*QueryGroup
	}
)
