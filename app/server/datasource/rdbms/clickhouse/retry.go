package clickhouse

import (
	"context"
	"errors"
	"strings"

	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
)

func ErrorCheckerMakeConnection(err error) bool {
	// Often happens at database startup
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// But for some reason context.DeadlineExceeded is not enough
	if strings.Contains(err.Error(), "context deadline exceeded") {
		return true
	}

	// For a some reason os.ErrDeadlineExceeded is not enough too
	if strings.Contains(err.Error(), "i/o timeout") {
		return true
	}

	return retry.ErrorCheckerMakeConnectionCommon(err)
}
