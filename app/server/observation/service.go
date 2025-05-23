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

	"github.com/ydb-platform/fq-connector-go/api/observation"
	"github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"
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

	err := s.storage.Close(context.Background())
	if err != nil {
		s.logger.Error("Error closing storage", zap.Error(err))
	} else {
		s.logger.Info("Storage closed successfully")
	}
}

// createSuccessError creates a success TError
func createSuccessError() *protos.TError {
	return &protos.TError{
		Status:  0, // SUCCESS
		Message: "Success",
	}
}

// createErrorFromErr creates a TError from an error
func createErrorFromErr(err error) *protos.TError {
	return &protos.TError{
		Status:  1, // GENERIC_ERROR
		Message: err.Error(),
	}
}

// ListIncomingQueries implements the gRPC method to stream incoming queries
func (s *serviceImpl) ListIncomingQueries(
	req *observation.ListIncomingQueriesRequest,
	stream observation.ObservationService_ListIncomingQueriesServer,
) error {
	logger := s.logger.With(
		zap.Int32("limit", req.Limit),
		zap.Int32("offset", req.Offset),
		zap.String("state", req.State.String()),
	)
	logger.Info("ListIncomingQueries request handling started")

	// Set default limit if not specified
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 1000 // Use a reasonable default
	}

	queries, err := s.storage.ListIncomingQueries(stream.Context(), logger, &req.State, limit, int(req.Offset))
	if err != nil {
		logger.Error("Failed to list incoming queries", zap.Error(err))
		// Send an error response
		errResponse := &observation.ListIncomingQueriesResponse{
			Query: nil,
			Error: createErrorFromErr(err),
		}
		if sendErr := stream.Send(errResponse); sendErr != nil {
			logger.Error("Failed to send error response", zap.Error(sendErr))
		}

		return status.Errorf(codes.Internal, "failed to list incoming queries: %v", err)
	}

	// Stream each query to the client
	for _, q := range queries {
		response := &observation.ListIncomingQueriesResponse{
			Query: q,
			Error: createSuccessError(),
		}
		if err := stream.Send(response); err != nil {
			logger.Error("Failed to send query to client", zap.Error(err))
			return status.Errorf(codes.Internal, "failed to send query to client: %v", err)
		}
	}

	logger.Info("ListIncomingQueries request handling finished", zap.Int("queries_sent", len(queries)))

	return nil
}

// ListOutgoingQueries implements the gRPC method to stream outgoing queries
func (s *serviceImpl) ListOutgoingQueries(
	req *observation.ListOutgoingQueriesRequest,
	stream observation.ObservationService_ListOutgoingQueriesServer,
) error {
	logger := s.logger.With(
		zap.String("incoming_query_id", req.IncomingQueryId),
		zap.Int32("limit", req.Limit),
		zap.Int32("offset", req.Offset),
		zap.String("state", req.State.String()),
	)
	logger.Info("ListOutgoingQueries request handling started")

	var incomingQueryIDParam *string

	if req.IncomingQueryId != "" {
		incomingQueryIDParam = &req.IncomingQueryId
	}

	// Set default limit if not specified
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 1000 // Use a reasonable default
	}

	queries, err := s.storage.ListOutgoingQueries(stream.Context(), logger, incomingQueryIDParam, &req.State, limit, int(req.Offset))
	if err != nil {
		logger.Error("Failed to list outgoing queries", zap.Error(err))
		// Send an error response
		errResponse := &observation.ListOutgoingQueriesResponse{
			Query: nil,
			Error: createErrorFromErr(err),
		}
		if sendErr := stream.Send(errResponse); sendErr != nil {
			logger.Error("Failed to send error response", zap.Error(sendErr))
		}

		return status.Errorf(codes.Internal, "failed to list outgoing queries: %v", err)
	}

	// Stream each query to the client
	for _, q := range queries {
		response := &observation.ListOutgoingQueriesResponse{
			Query: q,
			Error: createSuccessError(),
		}
		if err := stream.Send(response); err != nil {
			logger.Error("Failed to send query to client", zap.Error(err))
			return status.Errorf(codes.Internal, "failed to send query to client: %v", err)
		}
	}

	logger.Info("ListOutgoingQueries request handling finished", zap.Int("queries_sent", len(queries)))

	return nil
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
