package connector

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"

	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	"github.com/ydb-platform/fq-connector-go/app/server"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
)

type Server struct {
	launcher    *server.Launcher
	logger      *zap.Logger
	client      *clientImpl
	operational bool
	mutex       sync.Mutex
}

func (sn *Server) Start() {
	go func() {
		errChan := sn.launcher.Start()

		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

		for {
			select {
			case err := <-errChan:
				sn.mutex.Lock()
				if sn.operational {
					// Fail fast in case of fatal error
					if err != nil {
						sn.logger.Fatal("launcher start", zap.Error(err))
					}
				} else {
					return
				}
				sn.mutex.Unlock()
			case sig := <-signalChan:
				sn.logger.Info("interrupting signal", zap.Any("value", sig))
				sn.Stop()
			}
		}
	}()
}

func (sn *Server) Client() api_service.ConnectorClient {
	return sn.client
}

func (sn *Server) Stop() {
	sn.mutex.Lock()
	defer sn.mutex.Unlock()

	if sn.operational {
		sn.launcher.Stop()
		sn.client.stop()
		sn.operational = false
	}
}

func NewServer() (*Server, error) {
	cfg := server.NewDefaultConfig()
	logger := utils.NewDefaultLogger()

	launcher, err := server.NewLauncher(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("new server launcher: %w", err)
	}

	client, err := newClient(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	sn := &Server{
		launcher:    launcher,
		logger:      logger,
		operational: true,
		client:      client,
	}

	return sn, nil
}
