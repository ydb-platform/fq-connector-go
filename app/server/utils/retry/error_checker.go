package retry

import (
	"errors"
	"net"
	"os"
	"syscall"
)

type ErrorChecker func(err error) bool

func ErrorCheckerMakeConnectionCommon(err error) bool {
	// 'i/o timeout'
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	// 'connection refused'
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	// DNS errors typically caused by CI overload, like 'server misbehaving' and so on.
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		if dnsError.IsTemporary || dnsError.IsTimeout {
			return true
		}
	}

	return false
}

func ErrorCheckerNoop(_ error) bool {
	return false
}
