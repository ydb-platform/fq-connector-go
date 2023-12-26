package server

import (
	"context"
	"fmt"
	"net/http"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/library/go/core/metrics/solomon"
)

type serviceMetrics struct {
	httpServer *http.Server
	logger     *zap.Logger
	registry   *solomon.Registry
}

func (s *serviceMetrics) start() error {
	s.logger.Debug("starting HTTP metrics server", zap.String("address", s.httpServer.Addr))

	if err := s.httpServer.ListenAndServe(); err != nil {
		return fmt.Errorf("http metrics server listen and serve: %w", err)
	}

	return nil
}

func (s *serviceMetrics) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err := s.httpServer.Shutdown(ctx)
	if err != nil {
		s.logger.Error("shutdown http metrics server", zap.Error(err))
	}
}

func newServiceMetrics(logger *zap.Logger, cfg *config.TMetricsServerConfig, registry *solomon.Registry) service {
	mux := http.NewServeMux()
	mux.Handle("/metrics", NewHTTPPullerHandler(logger, registry, WithSpack()))

	httpServer := &http.Server{
		Addr:    utils.EndpointToString(cfg.Endpoint),
		Handler: mux,
	}

	// TODO: TLS
	logger.Warn("metrics server will use insecure connections")

	return &serviceMetrics{
		httpServer: httpServer,
		logger:     logger,
		registry:   registry,
	}
}
