package retry

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"syscall"
)

type ErrorChecker func(err error) bool

func ErrorCheckerMakeConnectionCommon(err error) bool {
	// 'i/o timeout'
	if strings.Contains(err.Error(), "i/o timeout") {
		return true
	}

	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		return true
	}

	// 'connection refused'
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	if strings.Contains(err.Error(), "connection refused") {
		return true
	}

	// DNS errors typically caused by CI overload, like 'server misbehaving' and so on.
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		if dnsError.IsTemporary || dnsError.IsTimeout {
			return true
		}
	}

	if strings.Contains(err.Error(), "server misbehaving") {
		return true
	}

	return false
}

func ErrorCheckerNoop(_ error) bool {
	return false
}
