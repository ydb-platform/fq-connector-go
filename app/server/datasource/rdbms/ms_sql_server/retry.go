package ms_sql_server

import (
	"strings"

	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
)

func ErrorCheckerMakeConnection(err error) bool {
	// For a some reason we get a string instead of wrapped syscall.ECONNREFUSED
	if strings.Contains(err.Error(), "connection refused") {
		return true
	}

	return retry.ErrorCheckerMakeConnectionCommon(err)
}
