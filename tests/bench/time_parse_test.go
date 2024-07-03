package bench

import (
	"testing"
	"time"

	"github.com/araddon/dateparse"
	"github.com/stretchr/testify/require"
)

func TestDateEquivalence(t *testing.T) {
	format := "2006-01-02"
	input := "1988-11-20"

	out1, err := time.Parse(format, input)
	require.NotNil(t, out1)
	require.NoError(t, err)

	out2, err := dateparse.ParseAny(input)
	require.NotNil(t, out2)
	require.NoError(t, err)

	require.True(t, out1.Equal(out2))
}

func TestTimeEquivalence(t *testing.T) {
	format := "2006-01-02 15:04:05.999999"
	input := "1988-11-20 11:12:13.444444"

	out1, err := time.Parse(format, input)
	require.NotNil(t, out1)
	require.NoError(t, err)

	out2, err := dateparse.ParseAny(input)
	require.NotNil(t, out2)
	require.NoError(t, err)

	require.True(t, out1.Equal(out2))
}

func BenchmarkDateParseStdLib(b *testing.B) {
	format := "2006-01-02"
	input := "1988-11-20"
	for i := 0; i < b.N; i++ {
		out, err := time.Parse(format, input)
		require.NotNil(b, out)
		require.NoError(b, err)
	}
}

func BenchmarkDateParseAraddonDateparse(b *testing.B) {
	input := "1988-11-20"
	for i := 0; i < b.N; i++ {
		out, err := dateparse.ParseStrict(input)
		require.NotNil(b, out)
		require.NoError(b, err)
	}
}

func BenchmarkTimeParseStdLib(b *testing.B) {
	format := "2006-01-02 15:04:05.999999"
	input := "1988-11-20 11:12:13.444444"
	for i := 0; i < b.N; i++ {
		out, err := time.Parse(format, input)
		require.NotNil(b, out)
		require.NoError(b, err)
	}
}

func BenchmarkTimeParseAraddonTimeparse(b *testing.B) {
	input := "1988-11-20 11:12:13.444444"
	for i := 0; i < b.N; i++ {
		out, err := dateparse.ParseAny(input)
		require.NotNil(b, out)
		require.NoError(b, err)
	}
}
