package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/library/go/core/metrics/solomon"
)

type serviceConnector struct {
	api_service.UnimplementedConnectorServer
	dataSourceCollection *DataSourceCollection
	cfg                  *config.TServerConfig
	grpcServer           *grpc.Server
	listener             net.Listener
	logger               *zap.Logger
}

func (*serviceConnector) ListTables(_ *api_service_protos.TListTablesRequest, _ api_service.Connector_ListTablesServer) error {
	return nil
}

func (s *serviceConnector) DescribeTable(
	ctx context.Context,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	logger := common.AnnotateLoggerForUnaryCall(s.logger, "DescribeTable", request.DataSourceInstance)
	logger.Info("request handling started", zap.String("table", request.GetTable()))

	if err := ValidateDescribeTableRequest(logger, request); err != nil {
		logger.Error("request handling failed", zap.Error(err))

		return &api_service_protos.TDescribeTableResponse{
			Error: common.NewAPIErrorFromStdError(err),
		}, nil
	}

	out, err := s.dataSourceCollection.DescribeTable(ctx, logger, request)
	if err != nil {
		logger.Error("request handling failed", zap.Error(err))

		out = &api_service_protos.TDescribeTableResponse{Error: common.NewAPIErrorFromStdError(err)}

		return out, nil
	}

	out.Error = common.NewSuccess()
	logger.Info("request handling finished", zap.String("response", out.String()))

	return out, nil
}

func (s *serviceConnector) ListSplits(request *api_service_protos.TListSplitsRequest, stream api_service.Connector_ListSplitsServer) error {
	logger := common.AnnotateLoggerWithMethod(s.logger, "ListSplits")
	logger.Info("request handling started", zap.Int("total selects", len(request.Selects)))

	if err := ValidateListSplitsRequest(logger, request); err != nil {
		return s.doListSplitsResponse(logger, stream,
			&api_service_protos.TListSplitsResponse{Error: common.NewAPIErrorFromStdError(err)})
	}

	// Make a trivial copy of requested selects
	totalSplits := 0

	for _, slct := range request.Selects {
		if err := s.doListSplitsHandleSelect(logger, stream, slct, &totalSplits); err != nil {
			logger.Error("request handling failed", zap.Error(err))

			return err
		}
	}

	logger.Info("request handling finished", zap.Int("total_splits", totalSplits))

	return nil
}

func (s *serviceConnector) doListSplitsHandleSelect(
	logger *zap.Logger,
	stream api_service.Connector_ListSplitsServer,
	slct *api_service_protos.TSelect,
	totalSplits *int,
) error {
	logger = common.AnnotateLoggerWithDataSourceInstance(logger, slct.DataSourceInstance)

	args := []zap.Field{
		zap.Int("split_id", *totalSplits),
	}
	args = append(args, common.SelectToFields(slct)...)

	logger.Debug("responding selects", args...)

	resp := &api_service_protos.TListSplitsResponse{
		Error:  common.NewSuccess(),
		Splits: []*api_service_protos.TSplit{{Select: slct}},
	}

	for _, split := range resp.Splits {
		args := []zap.Field{
			zap.Int("split_id", *totalSplits),
		}
		args = append(args, common.SelectToFields(split.Select)...)

		logger.Debug("responding split", args...)

		*totalSplits++
	}

	if err := s.doListSplitsResponse(logger, stream, resp); err != nil {
		return fmt.Errorf("do list splits response: %w", err)
	}

	return nil
}

func (*serviceConnector) doListSplitsResponse(
	logger *zap.Logger,
	stream api_service.Connector_ListSplitsServer,
	response *api_service_protos.TListSplitsResponse,
) error {
	if !common.IsSuccess(response.Error) {
		logger.Error("request handling failed", common.APIErrorToLogFields(response.Error)...)
	}

	if err := stream.Send(response); err != nil {
		logger.Error("send channel failed", zap.Error(err))

		return err
	}

	return nil
}

func (s *serviceConnector) ReadSplits(
	request *api_service_protos.TReadSplitsRequest,
	stream api_service.Connector_ReadSplitsServer,
) error {
	logger := common.AnnotateLoggerWithMethod(s.logger, "ReadSplits")
	logger.Info("request handling started", zap.Int("total_splits", len(request.Splits)))

	err := s.doReadSplits(logger, request, stream)
	if err != nil {
		logger.Error("request handling failed", zap.Error(err))

		response := &api_service_protos.TReadSplitsResponse{Error: common.NewAPIErrorFromStdError(err)}

		if err := stream.Send(response); err != nil {
			return fmt.Errorf("stream send: %w", err)
		}
	}

	logger.Info("request handling finished")

	return nil
}

func (s *serviceConnector) doReadSplits(
	logger *zap.Logger,
	request *api_service_protos.TReadSplitsRequest,
	stream api_service.Connector_ReadSplitsServer,
) error {
	if err := ValidateReadSplitsRequest(logger, request); err != nil {
		return fmt.Errorf("validate read splits request: %w", err)
	}

	for i, split := range request.Splits {
		splitLogger := common.
			AnnotateLoggerWithDataSourceInstance(logger, split.Select.DataSourceInstance).
			With(zap.Int("split_id", i))

		err := s.dataSourceCollection.DoReadSplit(
			splitLogger,
			stream,
			request,
			split,
		)

		if err != nil {
			return fmt.Errorf("read split %d: %w", i, err)
		}
	}

	return nil
}

func (s *serviceConnector) start() error {
	s.logger.Debug("starting GRPC server", zap.String("address", s.listener.Addr().String()))

	if err := s.grpcServer.Serve(s.listener); err != nil {
		return fmt.Errorf("listener serve: %w", err)
	}

	return nil
}

func makeGRPCOptions(logger *zap.Logger, cfg *config.TServerConfig, registry *solomon.Registry) ([]grpc.ServerOption, error) {
	var (
		opts      []grpc.ServerOption
		tlsConfig *config.TServerTLSConfig
	)

	unaryInterceptors := []grpc.UnaryServerInterceptor{UnaryServerMetrics(registry)}

	streamInterceptors := []grpc.StreamServerInterceptor{StreamServerMetrics(registry)}

	opts = append(opts, grpc.ChainUnaryInterceptor(unaryInterceptors...), grpc.ChainStreamInterceptor(streamInterceptors...))

	// TODO: drop deprecated fields after YQ-2057
	switch {
	case cfg.GetConnectorServer().GetTls() != nil:
		tlsConfig = cfg.GetConnectorServer().GetTls()
	case cfg.GetTls() != nil:
		tlsConfig = cfg.GetTls()
	default:
		logger.Warn("server will use insecure connections")

		return opts, nil
	}

	logger.Info("server will use TLS connections")

	logger.Debug("reading key pair", zap.String("cert", tlsConfig.Cert), zap.String("key", tlsConfig.Key))

	cert, err := tls.LoadX509KeyPair(tlsConfig.Cert, tlsConfig.Key)
	if err != nil {
		return nil, fmt.Errorf("load X509 key pair: %w", err)
	}

	// for security reasons we do not allow TLS < 1.2, see YQ-1877
	creds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	opts = append(opts, grpc.Creds(creds))

	return opts, nil
}

func (s *serviceConnector) stop() {
	s.grpcServer.GracefulStop()
}

func newServiceConnector(
	logger *zap.Logger,
	cfg *config.TServerConfig,
	registry *solomon.Registry,
) (service, error) {
	queryLoggerFactory := common.NewQueryLoggerFactory(cfg.Logger)

	// TODO: drop deprecated fields after YQ-2057
	var endpoint *api_common.TEndpoint

	switch {
	case cfg.GetConnectorServer().GetEndpoint() != nil:
		endpoint = cfg.ConnectorServer.GetEndpoint()
	case cfg.GetEndpoint() != nil:
		logger.Warn("Using deprecated field `endpoint` from config. Please update your config.")

		endpoint = cfg.GetEndpoint()
	default:
		return nil, fmt.Errorf("invalid config: no endpoint")
	}

	listener, err := net.Listen("tcp", common.EndpointToString(endpoint))
	if err != nil {
		return nil, fmt.Errorf("net listen: %w", err)
	}

	options, err := makeGRPCOptions(logger, cfg, registry)
	if err != nil {
		return nil, fmt.Errorf("make GRPC options: %w", err)
	}

	grpcServer := grpc.NewServer(options...)
	reflection.Register(grpcServer)

	s := &serviceConnector{
		dataSourceCollection: NewDataSourceCollection(
			queryLoggerFactory,
			memory.DefaultAllocator,
			paging.NewReadLimiterFactory(cfg.ReadLimit),
			cfg),
		logger:     logger,
		grpcServer: grpcServer,
		listener:   listener,
		cfg:        cfg,
	}

	api_service.RegisterConnectorServer(grpcServer, s)

	return s, nil
}
