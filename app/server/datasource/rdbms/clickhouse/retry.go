package clickhouse

import (
	"errors"
	"os"
	"syscall"
)

// //go:linkname decomposeDate time.(*Time).date
// func decomposeDate(*time.Time, bool) (year int, month int, day int, dayOfYear int)

// //go:linkname formatBits strconv.formatBits
// func formatBits([]byte, uint64, int, bool, bool) (b []byte, s string)

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
