package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	grpc_codes "google.golang.org/grpc/codes"
)

func RetriableErrorCheckerMakeConnection(_ error) bool {
	return false
}

func RetriableErrorCheckerQuery(err error) bool {
	switch {
	case ydb.IsTransportError(err, grpc_codes.ResourceExhausted):
		return true
	default:
		return false
	}
}
