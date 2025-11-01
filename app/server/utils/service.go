package utils //nolint:revive

// Service is an abstract interface representing some internal service
// running in a distinct thread.
type Service interface {
	Start() error
	Stop()
}
