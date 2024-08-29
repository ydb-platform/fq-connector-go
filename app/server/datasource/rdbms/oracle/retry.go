package oracle

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
)

func ErrorCheckerMakeConnection(err error) bool {
	// For a some reason poll.ErrNetClosed is unexported
	var opError *net.OpError
	if errors.As(err, &opError) {
		if strings.Contains(opError.Err.Error(), "use of closed network connection") {
			return true
		}
	}

	// Also happens on CI
	if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		return true
	}

	return retry.ErrorCheckerMakeConnectionCommon(err)
}
