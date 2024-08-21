package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

func TestRetry(t *testing.T) {
	cfg := &config.TExponentialBackoffConfig{
		InitialInterval:     "1ms",
		MaxInterval:         "5ms",
		RandomizationFactor: 0,
		Multiplier:          2,
		MaxElapsedTime:      "10ms",
	}

	t.Run("retriable", func(t *testing.T) {
		retrier := NewRetrierFromConfig(cfg,
			func(err error) bool {
				// all errors are retriable
				return true
			})

		logger := common.NewTestLogger(t)
		retriableErr := errors.New("some retriable error")

		err := retrier.Run(logger, func() error {
			return retriableErr
		})

		require.True(t, errors.Is(err, retriableErr))
	})

	t.Run("non-retriable", func(t *testing.T) {
		retrier := NewRetrierFromConfig(cfg,
			func(err error) bool {
				// all errors are non-retriable
				return false
			})

		logger := common.NewTestLogger(t)
		nonRetriableErr := errors.New("some non-retriable error")

		err := retrier.Run(logger, func() error {
			return nonRetriableErr
		})

		require.True(t, errors.Is(err, nonRetriableErr))
	})

	t.Run("context deadline exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()

		logger := common.NewTestLogger(t)
		retriableErr := errors.New("some retriable error")

		retrier := NewRetrierFromConfig(cfg,
			func(err error) bool {
				return !errors.Is(err, context.DeadlineExceeded)
			})

		err := retrier.Run(logger, func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return retriableErr
			}
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, ctx.Err()), err)
	})
}
