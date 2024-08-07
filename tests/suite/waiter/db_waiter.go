package waiter

import (
	"github.com/ydb-platform/fq-connector-go/app/config"
)

type DbWaiter interface {
	Wait() error
}

type DefaultDbWaiter struct {
	retrier retrier
}

func (w *DefaultDbWaiter) Wait() error {
	return w.retrier.run()
}

func newDefaultExponentialBackoffConfig() *config.TExponentialBackoffConfig {
	// return &config.TExponentialBackoffConfig{
	// 	InitialInterval:     "1ms",
	// 	MaxInterval:         "5ms",
	// 	RandomizationFactor: 0,
	// 	Multiplier:          2,
	// 	MaxElapsedTime:      "10ms",
	// }
	return &config.TExponentialBackoffConfig{
		InitialInterval:     "500ms",
		RandomizationFactor: 0.5,
		Multiplier:          1.5,
		MaxInterval:         "20s",
		MaxElapsedTime:      "1m",
	}
}

func NewDefaultDBWaiter(
	dsFuncs DataSourceRetrierFuncs,
) *DefaultDbWaiter {
	cfg := newDefaultExponentialBackoffConfig()
	return &DefaultDbWaiter{
		retrier: newDbRetrier(dsFuncs, cfg),
	}
}
