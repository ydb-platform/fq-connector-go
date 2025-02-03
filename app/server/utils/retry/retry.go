package retry

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

type Operation func() error

type Retrier interface {
	Run(ctx context.Context, logger *zap.Logger, op Operation) error
}

type backoffFactory func() *backoff.ExponentialBackOff

type retrierDefault struct {
	retriableErrorChecker ErrorChecker
	backoffFactory        backoffFactory
}

func (r *retrierDefault) Run(ctx context.Context, logger *zap.Logger, op Operation) error {
	var attempts int

	return backoff.Retry(backoff.Operation(func() error {
		attempts++

		err := op()

		if err != nil {
			// It's convinient to disable retries for negative tests.
			// These tests are marked with 'ForbidRetries' flag in GRPC Metadata.
			md, mdExists := metadata.FromIncomingContext(ctx)
			if mdExists {
				if _, flagSet := md[common.ForbidRetries]; flagSet {
					logger.Warn("retriable error occurred, but 'ForbidRetries' flag was set", zap.Error(err))

					return backoff.Permanent(err)
				}
			}

			// Check if error is retriable
			if r.retriableErrorChecker(err) {
				logger.Warn("retriable error occurred", zap.Error(err), zap.Int("attempts", attempts))

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
		retriableErrorChecker: func(_ error) bool {
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
