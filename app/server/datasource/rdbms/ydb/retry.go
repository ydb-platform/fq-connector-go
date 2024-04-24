package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"google.golang.org/grpc/codes"
)

func RetriableErrorChecker(err error) bool {
	switch {
	case ydb.IsTransportError(err, codes.ResourceExhausted):
		return true
	default:
		return false
	}
}
