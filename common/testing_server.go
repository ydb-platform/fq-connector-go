package common

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

// TestingServer is mainly used in integration tests or benchmarks.
// It may be either a real Connector server embedded in this process,
// or a Connector client connected to the remote server instance.
type TestingServer interface {
	Start()
	ClientBuffering() *ClientBuffering
	ClientStreaming() *ClientStreaming
	MetricsSnapshot() (*MetricsSnapshot, error)
	Stop()
}

type testingServerRemote struct {
}

func (ts *testingServerRemote) Start() {
}

func (ts *testingServerRemote) ClientBuffering() *common.ClientBuffering {
	panic("not implemented") // TODO: Implement
}

func (ts *testingServerRemote) ClientStreaming() *common.ClientStreaming {
	panic("not implemented") // TODO: Implement
}

func (ts *testingServerRemote) MetricsSnapshot() (*common.MetricsSnapshot, error) {
	panic("not implemented") // TODO: Implement
}

func (ts *testingServerRemote) Stop() {
	panic("not implemented") // TODO: Implement
}

func NewTestingServerEmbedded(logger *zap.Logger, clientCfg *config.TClientConfig) (TestingServer, error) {
	clientStreaming, err := NewClientBufferingFromClientConfig(logger, clientCfg)
	if err != nil {
		return nil, fmt.Errorf("new client streaming from client config")
	}

	clientBuffering, err := NewClientBufferingFromClientConfig(logger, clientCfg)
	if err != nil {
		return nil, fmt.Errorf("new client buffering from client config")
	}

}
