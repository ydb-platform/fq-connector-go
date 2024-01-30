package server

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

// Embedded server is used in different kinds of tests, when it is important
// to launch server in the same process with the tests itself.
type Embedded struct {
	launcher        *Launcher
	logger          *zap.Logger
	clientBuffering *common.ClientBuffering
	clientStreaming *common.ClientStreaming
	operational     bool
	mutex           sync.Mutex
}

func (s *Embedded) Start() {
	go func() {
		errChan := s.launcher.Start()

		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

		for {
			select {
			case err := <-errChan:
				s.handleStartError(err)
			case sig := <-signalChan:
				s.logger.Info("interrupting signal", zap.Any("value", sig))
				s.Stop()
			}
		}
	}()
}

func (s *Embedded) handleStartError(err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.operational {
		// Fail fast in case of fatal error
		if err != nil {
			s.logger.Fatal("launcher start", zap.Error(err))
		}
	} else {
		s.logger.Warn("launcher start", zap.Error(err))
	}
}

func (s *Embedded) ClientBuffering() *common.ClientBuffering { return s.clientBuffering }

func (s *Embedded) ClientStreaming() *common.ClientStreaming { return s.clientStreaming }

func (s *Embedded) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.operational {
		s.launcher.Stop()
		s.clientBuffering.Close()
		s.operational = false
	}
}

type ServerOption interface {
	apply(cfg *config.TServerConfig)
}

type withPagingConfig struct {
	pagingConfig *config.TPagingConfig
}

func (o *withPagingConfig) apply(cfg *config.TServerConfig) {
	cfg.Paging = o.pagingConfig
}

func WithPagingConfig(pagingConfig *config.TPagingConfig) ServerOption {
	return &withPagingConfig{pagingConfig: pagingConfig}
}

type withLoggerConfig struct {
	loggerConfig *config.TLoggerConfig
}

func (o *withLoggerConfig) apply(cfg *config.TServerConfig) {
	cfg.Logger = o.loggerConfig
}

func WithLoggerConfig(loggerConfig *config.TLoggerConfig) ServerOption {
	return &withLoggerConfig{loggerConfig: loggerConfig}
}

type withPprofServerConfig struct {
	pprofServerConfig *config.TPprofServerConfig
}

func (o *withPprofServerConfig) apply(cfg *config.TServerConfig) {
	cfg.PprofServer = o.pprofServerConfig
}

func WithPprofServerConfig(pprofServerConfig *config.TPprofServerConfig) ServerOption {
	return &withPprofServerConfig{pprofServerConfig: pprofServerConfig}
}

func NewEmbedded(options ...ServerOption) (*Embedded, error) {
	cfg := NewDefaultConfig()
	for _, o := range options {
		o.apply(cfg)
	}

	logger, err := common.NewLoggerFromConfig(cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("new logger from config: %w", err)
	}

	launcher, err := NewLauncher(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("new server launcher: %w", err)
	}

	clientBuffering, err := common.NewClientBufferingFromServerConfig(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	clientStreaming, err := common.NewClientStreamingFromServerConfig(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	sn := &Embedded{
		launcher:        launcher,
		logger:          logger,
		operational:     true,
		clientBuffering: clientBuffering,
		clientStreaming: clientStreaming,
	}

	return sn, nil
}
