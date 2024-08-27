package postgresql

import (
	"context"
	"strings"

	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
)

func ErrorCheckerMakeConnection(err error) bool {
	// Happens instead of 'i/o timeout'
	if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		return true
	}

	return retry.ErrorCheckerMakeConnectionCommon(err)
}
