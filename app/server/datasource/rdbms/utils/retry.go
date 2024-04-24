package utils

import (
	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

type Operation func() error

type RetriableErrorChecker func(err error) bool

type Retrier interface {
	Run(logger *zap.Logger, op Operation) error
}

type backoffFactory func() *backoff.ExponentialBackOff

type retrierDefault struct {
	retriableErrorChecker RetriableErrorChecker
	backoffFactory        backoffFactory
}

func (r *retrierDefault) Run(logger *zap.Logger, op Operation) error {
	return backoff.Retry(backoff.Operation(func() error {
		err := op()

		if err != nil {
			if r.retriableErrorChecker(err) {
				logger.Warn("retriable error occured", zap.Error(err))

				return err
			}

			return backoff.Permanent(err)
		}

		return nil
	}), r.backoffFactory())
}

func NewRetrierFromConfig(cfg *config.TExponentialBackoffConfig, retriableErrorChecker RetriableErrorChecker) Retrier {
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
			// all errors are non-retriable by defatul
			return false
		},
		backoffFactory: func() *backoff.ExponentialBackOff {
			return &backoff.ExponentialBackOff{}
		},
	}
}
