package ydb

import (
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	grpc_codes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
)

func ErrorCheckerMakeConnection(err error) bool {
	if strings.Contains(err.Error(), "server misbehaving") {
		return true
	}

	return retry.ErrorCheckerMakeConnectionCommon(err)
}

func ErrorCheckerQuery(err error) bool {
	switch {
	case ydb.IsTransportError(err, grpc_codes.ResourceExhausted):
		return true
	default:
		return false
	}
}
