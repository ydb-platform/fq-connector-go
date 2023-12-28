package server

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/library/go/core/metrics/solomon"
)

type service interface {
	start() error
	stop()
}

type Launcher struct {
	services map[string]service
	logger   *zap.Logger
}

func (l *Launcher) Start() <-chan error {
	errChan := make(chan error, len(l.services))

	for key := range l.services {
		go func(key string) {
			l.logger.Info("starting service", zap.String("service", key))

			// blocking call
			errChan <- l.services[key].start()
		}(key)
	}

	return errChan
}

func (l *Launcher) Stop() {
	for key, s := range l.services {
		l.logger.Info("stopping service", zap.String("service", key))
		s.stop()
	}
}

const (
	connectorServiceKey = "connector"
	pprofServiceKey     = "pprof"
	metricsKey          = "metrics"
)

func NewLauncher(logger *zap.Logger, cfg *config.TServerConfig) (*Launcher, error) {
	l := &Launcher{
		services: make(map[string]service, 2),
		logger:   logger,
	}

	var err error

	registry := solomon.NewRegistry(&solomon.RegistryOpts{
		Separator:  '.',
		UseNameTag: true,
	})

	if cfg.MetricsServer != nil {
		l.services[metricsKey] = newServiceMetrics(
			logger.With(zap.String("service", metricsKey)),
			cfg.MetricsServer, registry)
	}

	// init GRPC server
	l.services[connectorServiceKey], err = newServiceConnector(
		logger.With(zap.String("service", connectorServiceKey)),
		cfg, registry)
	if err != nil {
		return nil, fmt.Errorf("new connector server: %w", err)
	}

	// init Pprof server
	if cfg.PprofServer != nil {
		l.services[pprofServiceKey] = newServicePprof(
			logger.With(zap.String("service", pprofServiceKey)),
			cfg.PprofServer)
	}

	return l, nil
}

func StartLauncherAndWaitForSignalOrError(logger *zap.Logger, l *Launcher) {
	errChan := l.Start()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errChan:
		if err != nil {
			logger.Error("service fatal error", zap.Error(err))
		}
	case sig := <-signalChan:
		logger.Info("interrupting signal", zap.Any("value", sig))
	}

	l.Stop()
}
