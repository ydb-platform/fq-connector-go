package retry

import (
	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

type Operation func() error

type Retrier interface {
	Run(logger *zap.Logger, op Operation) error
}

type backoffFactory func() *backoff.ExponentialBackOff

type retrierDefault struct {
	retriableErrorChecker ErrorChecker
	backoffFactory        backoffFactory
}

func (r *retrierDefault) Run(logger *zap.Logger, op Operation) error {
	return backoff.Retry(backoff.Operation(func() error {
		err := op()

		if err != nil {
			if r.retriableErrorChecker(err) {
				logger.Warn("retriable error occurred", zap.Error(err))

				return err
			}

			return backoff.Permanent(err)
		}

		return nil
	}), r.backoffFactory())
}

func NewRetrierFromConfig(cfg *config.TExponentialBackoffConfig, retriableErrorChecker ErrorChecker) Retrier {
	return &retrierDefault{
		retriableErrorChecker: retriableErrorChecker,
		backoffFactory: func() *backoff.ExponentialBackOff {
			b := backoff.NewExponentialBackOff()
			b.MaxElapsedTime = common.MustDurationFromString(cfg.MaxElapsedTime)
			b.InitialInterval = common.MustDurationFromString(cfg.InitialInterval)
			b.MaxInterval = common.MustDurationFromString(cfg.MaxInterval)
			b.RandomizationFactor = cfg.RandomizationFactor
			b.Multiplier = cfg.Multiplier
			b.Reset()

			return b
		},
	}
}

func NewRetrierNoop() Retrier {
	return &retrierDefault{
		retriableErrorChecker: func(err error) bool {
			// all errors are non-retriable by default
			return false
		},
		backoffFactory: func() *backoff.ExponentialBackOff {
			return backoff.NewExponentialBackOff()
		},
	}
}

type RetrierSet struct {
	MakeConnection Retrier
	Query          Retrier
}

func NewRetrierSetNoop() *RetrierSet {
	return &RetrierSet{
		MakeConnection: NewRetrierNoop(),
		Query:          NewRetrierNoop(),
	}
}
