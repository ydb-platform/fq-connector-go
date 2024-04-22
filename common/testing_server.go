package common

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
