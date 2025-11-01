package common //nolint:revive

import (
	"errors"
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
	clientBuffering *ClientBuffering
	clientStreaming *ClientStreaming
}

func (*testingServerRemote) Start() {}

func (ts *testingServerRemote) ClientBuffering() *ClientBuffering { return ts.clientBuffering }

func (ts *testingServerRemote) ClientStreaming() *ClientStreaming { return ts.clientStreaming }

func (*testingServerRemote) MetricsSnapshot() (*MetricsSnapshot, error) {
	return nil, errors.New("not implemented")
}

func (ts *testingServerRemote) Stop() {
	ts.clientBuffering.Close()
	ts.clientStreaming.Close()
}

func NewTestingServerRemote(logger *zap.Logger, clientCfg *config.TClientConfig) (TestingServer, error) {
	clientBuffering, err := NewClientBufferingFromClientConfig(logger, clientCfg)
	if err != nil {
		return nil, fmt.Errorf("new client streaming from client config: %w", err)
	}

	clientStreaming, err := NewClientStreamingFromClientConfig(logger, clientCfg)
	if err != nil {
		return nil, fmt.Errorf("new client buffering from client config: %w", err)
	}

	return &testingServerRemote{
		clientBuffering: clientBuffering,
		clientStreaming: clientStreaming,
	}, nil
}
