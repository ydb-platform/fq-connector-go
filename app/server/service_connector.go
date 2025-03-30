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
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"
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
	logger := utils.LoggerMustFromContext(ctx)
	logger = common.AnnotateLoggerWithDataSourceInstance(logger, request.DataSourceInstance)
	logger.Info("request handling started", zap.String("table", request.GetTable()))

	if err := ValidateDescribeTableRequest(logger, request); err != nil {
		logger.Error("request handling failed", zap.Error(err))

		response := &api_service_protos.TDescribeTableResponse{
			Error: common.NewAPIErrorFromStdError(err, request.DataSourceInstance.Kind),
		}

		return response, nil
	}

	out, err := s.dataSourceCollection.DescribeTable(ctx, logger, request)
	if err != nil {
		logger.Error("request handling failed", zap.Error(err))

		out = &api_service_protos.TDescribeTableResponse{Error: common.NewAPIErrorFromStdError(err, request.DataSourceInstance.Kind)}

		return out, nil
	}

	out.Error = common.NewSuccess()
	logger.Info("request handling finished", zap.String("response", out.String()))

	return out, nil
}

func (s *serviceConnector) ListSplits(
	request *api_service_protos.TListSplitsRequest,
	stream api_service.Connector_ListSplitsServer,
) error {
	logger := utils.LoggerMustFromContext(stream.Context())
	logger.Info("request handling started", zap.Int("total selects", len(request.Selects)))

	if err := ValidateListSplitsRequest(logger, request); err != nil {
		return s.doListSplitsResponse(logger, stream,
			&api_service_protos.TListSplitsResponse{
				Error: common.NewAPIErrorFromStdError(
					err,
					api_common.EGenericDataSourceKind_DATA_SOURCE_KIND_UNSPECIFIED,
				),
			},
		)
	}

	if err := s.dataSourceCollection.ListSplits(logger, stream, request); err != nil {
		return s.doListSplitsResponse(logger, stream,
			&api_service_protos.TListSplitsResponse{
				Error: common.NewAPIErrorFromStdError(
					err,
					api_common.EGenericDataSourceKind_DATA_SOURCE_KIND_UNSPECIFIED,
				),
			},
		)
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
	logger := utils.LoggerMustFromContext(stream.Context())
	logger.Info("request handling started", zap.Int("total_splits", len(request.Splits)))

	var err error
	logger, err = s.doReadSplits(logger, request, stream)

	if err != nil {
		logger.Error("request handling failed", zap.Error(err))

		response := &api_service_protos.TReadSplitsResponse{
			Error: common.NewAPIErrorFromStdError(
				err,
				api_common.EGenericDataSourceKind_DATA_SOURCE_KIND_UNSPECIFIED,
			),
		}

		if err := stream.Send(response); err != nil {
			return fmt.Errorf("stream send: %w", err)
		}
	} else {
		logger.Info("request handling finished")
	}

	return nil
}

func (s *serviceConnector) doReadSplits(
	logger *zap.Logger,
	request *api_service_protos.TReadSplitsRequest,
	stream api_service.Connector_ReadSplitsServer,
) (*zap.Logger, error) {
	if err := ValidateReadSplitsRequest(logger, request); err != nil {
		return logger, fmt.Errorf("validate read splits request: %w", err)
	}

	for _, split := range request.Splits {
		splitLogger := common.
			AnnotateLoggerWithDataSourceInstance(logger, split.Select.DataSourceInstance).
			With(zap.Uint64("id", split.Id))

		err := s.dataSourceCollection.ReadSplit(
			splitLogger,
			stream,
			request,
			split,
		)

		if err != nil {
			return splitLogger, fmt.Errorf("read split %d: %w", split.Id, err)
		}
	}

	return logger, nil
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

	unaryInterceptors := []grpc.UnaryServerInterceptor{UnaryServerMetrics(logger, registry), utils.UnaryServerMetadata(logger)}

	streamInterceptors := []grpc.StreamServerInterceptor{StreamServerMetrics(logger, registry), utils.StreamServerMetadata(logger)}

	opts = append(opts, grpc.ChainUnaryInterceptor(unaryInterceptors...), grpc.ChainStreamInterceptor(streamInterceptors...))

	// YQ-3686: tune message size limit, default 4 MBs are not enough
	opts = append(opts, grpc.MaxRecvMsgSize(int(cfg.ConnectorServer.MaxRecvMessageSize)))

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
	common.LogCloserError(s.logger, s.dataSourceCollection, "closing data source collection")
}

func newServiceConnector(
	logger *zap.Logger,
	cfg *config.TServerConfig,
	registry *solomon.Registry,
) (service, error) {
	queryLoggerFactory := common.NewQueryLoggerFactory(cfg.Logger)

	// TODO: drop deprecated fields after YQ-2057
	var endpoint *api_common.TGenericEndpoint

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

	dataSourceCollection, err := NewDataSourceCollection(
		queryLoggerFactory,
		memory.DefaultAllocator,
		paging.NewReadLimiterFactory(cfg.ReadLimit),
		conversion.NewCollection(cfg.Conversion),
		cfg,
	)
	if err != nil {
		return nil, fmt.Errorf("new data source collection: %w", err)
	}

	s := &serviceConnector{
		dataSourceCollection: dataSourceCollection,
		logger:               logger,
		grpcServer:           grpcServer,
		listener:             listener,
		cfg:                  cfg,
	}

	api_service.RegisterConnectorServer(grpcServer, s)

	return s, nil
}
