package ydb

import (
	grpc_codes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func ErrorCheckerQuery(err error) bool {
	switch {
	case ydb.IsTransportError(err, grpc_codes.ResourceExhausted):
		return true
	default:
		return false
	}
}
