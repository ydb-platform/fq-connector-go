package common //nolint:revive

import (
	"fmt"
	"runtime"
)

func PrintStackTrace() {
	var (
		buf = make([]byte, 1024)
		n   int
	)

	for {
		n = runtime.Stack(buf, false)
		if n < len(buf) {
			break
		}

		buf = make([]byte, 2*len(buf))
	}

	fmt.Printf("Stack Trace:\n%s\n", string(buf[:n]))
}
