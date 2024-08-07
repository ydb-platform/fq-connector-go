package waiter

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

type Operation func() error

type RetriableErrorChecker func(err error) bool

type backoffFactory func() *backoff.ExponentialBackOff

type retrier interface {
	run() error
}

var _ retrier = (*dbRetrier)(nil)

type dbRetrier struct {
	op                    Operation
	isRetriableErrorCheck RetriableErrorChecker
	backoffFactory        backoffFactory
}

func (r *dbRetrier) run() error {
	return backoff.Retry(backoff.Operation(func() error {
		err := r.op()

		if err != nil {
			if r.isRetriableErrorCheck(err) {
				return err
			}

			return backoff.Permanent(err)
		}

		return nil
	}), r.backoffFactory())
}

func newBackOffFactory(cfg *config.TExponentialBackoffConfig) backoffFactory {
	return func() *backoff.ExponentialBackOff {
		b := backoff.NewExponentialBackOff()
		b.MaxElapsedTime = common.MustDurationFromString(cfg.MaxElapsedTime)
		b.InitialInterval = common.MustDurationFromString(cfg.InitialInterval)
		b.MaxInterval = common.MustDurationFromString(cfg.MaxInterval)
		b.RandomizationFactor = cfg.RandomizationFactor
		b.Multiplier = cfg.Multiplier
		b.Reset()

		return b
	}
}

func newDbRetrier(dsFuncs DataSourceRetrierFuncs, cfg *config.TExponentialBackoffConfig) *dbRetrier {
	return &dbRetrier{
		op:                    dsFuncs.Op,
		isRetriableErrorCheck: dsFuncs.IsRetriableError,
		backoffFactory:        newBackOffFactory(cfg),
	}
}
