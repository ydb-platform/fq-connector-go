package server

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/observation"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/library/go/core/metrics/solomon"
)

type Launcher struct {
	services map[string]utils.Service
	logger   *zap.Logger
}

func (l *Launcher) Start() <-chan error {
	errChan := make(chan error, len(l.services))

	for key := range l.services {
		go func(key string) {
			// blocking call
			errChan <- l.services[key].Start()
		}(key)
	}

	return errChan
}

func (l *Launcher) Stop() {
	for key, s := range l.services {
		l.logger.Info("stopping service", zap.String("service", key))
		s.Stop()
	}
}

const (
	connectorServiceKey   = "connector"
	pprofServiceKey       = "pprof"
	metricsServiceKey     = "metrics"
	observationServiceKey = "observation"
)

func NewLauncher(logger *zap.Logger, cfg *config.TServerConfig) (*Launcher, error) {
	l := &Launcher{
		services: make(map[string]utils.Service, 3),
		logger:   logger,
	}

	var err error

	// initialize storage for solomon metrics
	solomonRegistry := solomon.NewRegistry(&solomon.RegistryOpts{
		Separator:  '.',
		UseNameTag: true,
	})

	// initialize storage for query observation system
	observationStorage, err := observation.NewStorage(cfg.Observation)
	if err != nil {
		return nil, fmt.Errorf("new observation storage: %w", err)
	}

	// init metrics server
	if cfg.MetricsServer != nil {
		l.services[metricsServiceKey] = newServiceMetrics(
			logger.With(zap.String("service", metricsServiceKey)),
			cfg.MetricsServer, solomonRegistry)
	}

	// init GRPC server
	l.services[connectorServiceKey], err = newServiceConnector(
		logger.With(zap.String("service", connectorServiceKey)),
		cfg,
		solomonRegistry,
		&observationStorage,
	)
	if err != nil {
		return nil, fmt.Errorf("new connector service: %w", err)
	}

	// init Pprof server
	if cfg.PprofServer != nil {
		l.services[pprofServiceKey] = newServicePprof(
			logger.With(zap.String("service", pprofServiceKey)),
			cfg.PprofServer)
	}

	// init Observation server
	if cfg.Observation != nil {
		l.services[observationServiceKey], err = observation.NewService(
			logger.With(zap.String("service", observationServiceKey)),
			cfg.Observation,
			observationStorage,
		)
		if err != nil {
			return nil, fmt.Errorf("new observation service: %w", err)
		}
	}

	return l, nil
}

func startLauncherAndWaitForSignalOrError(logger *zap.Logger, l *Launcher) {
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
