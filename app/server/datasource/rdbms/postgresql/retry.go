package postgresql

import (
	"context"
	"errors"

	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
)

func ErrorCheckerMakeConnection(err error) bool {
	// Happens instead of 'i/o timeout'
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	return retry.ErrorCheckerMakeConnectionCommon(err)
}
