package connector

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/client"
	"github.com/ydb-platform/fq-connector-go/app/common"
	"github.com/ydb-platform/fq-connector-go/app/server"
)

type Server struct {
	launcher    *server.Launcher
	logger      *zap.Logger
	client      client.Client
	operational bool
	mutex       sync.Mutex
}

func (s *Server) Start() {
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

func (s *Server) handleStartError(err error) {
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

func (s *Server) Client() client.Client {
	return s.client
}

func (s *Server) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.operational {
		s.launcher.Stop()
		s.client.Close()
		s.operational = false
	}
}

func NewServer() (*Server, error) {
	cfg := server.NewDefaultConfig()
	logger := common.NewDefaultLogger()

	launcher, err := server.NewLauncher(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("new server launcher: %w", err)
	}

	cl, err := client.NewClientFromServerConfig(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	sn := &Server{
		launcher:    launcher,
		logger:      logger,
		operational: true,
		client:      cl,
	}

	return sn, nil
}
