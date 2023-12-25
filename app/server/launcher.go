package server

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/library/go/core/metrics/solomon"
	"go.uber.org/zap"
)

type service interface {
	start() error
	stop()
}

type launcher struct {
	services map[string]service
	logger   *zap.Logger
}

func (l *launcher) start() <-chan error {
	errChan := make(chan error, len(l.services))

	for key := range l.services {
		key := key
		go func(key string) {
			l.logger.Info("starting service", zap.String("service", key))

			// blocking call
			errChan <- l.services[key].start()
		}(key)
	}

	return errChan
}

func (l *launcher) stop() {
	// TODO: make it concurrent
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

func newLauncher(logger *zap.Logger, cfg *config.TServerConfig) (*launcher, error) {
	l := &launcher{
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

func run(cmd *cobra.Command, _ []string) error {
	configPath, err := cmd.Flags().GetString(configFlag)
	if err != nil {
		return fmt.Errorf("get config flag: %v", err)
	}

	cfg, err := newConfigFromPath(configPath)
	if err != nil {
		return fmt.Errorf("new config: %w", err)
	}

	logger, err := utils.NewLoggerFromConfig(cfg.Logger)
	if err != nil {
		return fmt.Errorf("new logger from config: %w", err)
	}

	l, err := newLauncher(logger, cfg)
	if err != nil {
		return fmt.Errorf("new launcher: %w", err)
	}

	errChan := l.start()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errChan:
		logger.Error("service fatal error", zap.Error(err))
	case sig := <-signalChan:
		logger.Info("interrupting signal", zap.Any("value", sig))
		l.stop()
	}

	return nil
}
