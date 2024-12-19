// go test -v -cpu=4 -run=none -bench=. -benchmem strftime_test.go
package strftime_test

import (
	"testing"
	"time"

	"github.com/phuslu/fasttime"
	"github.com/stretchr/testify/require"
)

var now = time.Now().UTC()

func TestEquivalence(t *testing.T) {
	std1 := now.Format(time.RFC3339Nano)
	std2 := now.Format("2006-01-02T15:04:05.999999999Z")
	fast := fasttime.Strftime("%Y-%m-%dT%H:%M:%S.%N%:z", now)
	require.Equal(t, std1, fast)
	require.Equal(t, std2, fast)
}

func BenchmarkRFC3339StdTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		now.Format(time.RFC3339Nano)
	}
}

func BenchmarkRFC3339Fasttime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fasttime.Strftime("%Y-%m-%dT%H:%M:%S%N:z", now)
	}
}
