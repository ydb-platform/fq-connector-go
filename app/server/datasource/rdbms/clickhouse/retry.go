package clickhouse

import (
	"errors"
	"os"
	"syscall"
)

func RetriableErrorCheckerMakeConnection(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	return false
}

func RetriableErrorCheckerQuery(_ error) bool {
	return false
}
