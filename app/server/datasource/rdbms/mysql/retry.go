package mysql

import (
	"strings"

	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
)

func ErrorCheckerMakeConnection(err error) bool {
	// For a some reason sys.ECONNREFUSED is not enough
	if strings.Contains(err.Error(), "connection refused") {
		return true
	}

	return retry.ErrorCheckerMakeConnectionCommon(err)
}
