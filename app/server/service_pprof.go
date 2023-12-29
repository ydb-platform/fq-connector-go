package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
)

type servicePprof struct {
	httpServer *http.Server
	logger     *zap.Logger
}

func (s *servicePprof) start() error {
	s.logger.Debug("starting HTTP server", zap.String("address", s.httpServer.Addr))

	if err := s.httpServer.ListenAndServe(); err != nil {
		return fmt.Errorf("http server listen and server: %w", err)
	}

	return nil
}

const shutdownTimeout = 5 * time.Second

func (s *servicePprof) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err := s.httpServer.Shutdown(ctx)
	if err != nil {
		s.logger.Error("shutdown http server", zap.Error(err))
	}
}

func newServicePprof(logger *zap.Logger, cfg *config.TPprofServerConfig) service {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	httpServer := &http.Server{
		Addr:    common.EndpointToString(cfg.Endpoint),
		Handler: mux,
	}

	// TODO: TLS
	logger.Warn("server will use insecure connections")

	return &servicePprof{
		httpServer: httpServer,
		logger:     logger,
	}
}
