package conversion

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDateToStringConverter(t *testing.T) {
	testCases := []time.Time{
		time.Date(math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt, time.UTC),
		time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
		time.Date(math.MinInt, math.MinInt, math.MinInt, math.MinInt, math.MinInt, math.MinInt, math.MinInt, time.UTC),
		time.Date(1950, 5, 27, 0, 0, 0, 0, time.UTC),
		time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(100500, 5, 20, -12, -55, -8, 0, time.UTC),
		time.Date(1988, 11, 20, 12, 55, 8, 0, time.UTC),
		time.Date(100, 2, 3, 4, 5, 6, 7, time.UTC),
		time.Date(10, 2, 3, 4, 5, 6, 7, time.UTC),
		time.Date(1, 2, 3, 4, 5, 6, 7, time.UTC),
		time.Date(-100500, -10, -35, -8, -2000, -300000, -50404040, time.UTC),
	}

	const format = "2006-01-02"

	var (
		converterUnsafe  dateToStringConverterUnsafe
		converterDefault dateToStringConverter
	)

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.Format(format), func(t *testing.T) {
			// Check equivalence of results produced by default and unsafe converters
			expectedOut, err := converterDefault.Convert(&tc)
			require.NoError(t, err)
			actualOut, err := converterUnsafe.Convert(&tc)
			require.NoError(t, err)
			require.Equal(t, expectedOut, actualOut)
		})
	}
}

func BenchmarkDateToStringConverter(b *testing.B) {
	t := time.Now()

	b.Run("Default", func(b *testing.B) {
		var converter dateToStringConverter

		for i := 0; i < b.N; i++ {
			out, err := converter.Convert(&t)
			if err != nil {
				b.Fatal(err)
			}
			_ = out
		}
	})

	b.Run("Unsafe", func(b *testing.B) {
		var converter dateToStringConverterUnsafe

		for i := 0; i < b.N; i++ {
			out, err := converter.Convert(&t)
			if err != nil {
				b.Fatal(err)
			}
			_ = out
		}
	})
}

func FuzzDateToStringConverter(f *testing.F) {
	var (
		converterUnsafe  dateToStringConverterUnsafe
		converterDefault dateToStringConverter
	)

	f.Add(100500, 5, 20, -12, -55, -8, 0)
	f.Add(1988, 11, 20, 12, 55, 8, 0)
	f.Add(100, 2, 3, 4, 5, 6, 7)
	f.Add(10, 2, 3, 4, 5, 6, 7)
	f.Add(1, 2, 3, 4, 5, 6, 7)
	f.Add(-1, 1, 1, 0, 0, 0, 0)
	f.Add(-100500, -10, -35, -8, -2000, -300000, -50404040)

	f.Fuzz(func(t *testing.T, year int, month int, day int, hour int, min int, sec int, nsec int) {
		in := time.Date(year, time.Month(month), day, hour, min, sec, nsec, time.UTC)
		expectedOut, err := converterDefault.Convert(&in)
		require.NoError(t, err)
		actualOut, err := converterUnsafe.Convert(&in)
		require.NoError(t, err)
		require.Equal(t, expectedOut, actualOut)
	})
}

func TestTimestampToStringConverter(t *testing.T) {
	testCases := []time.Time{
		time.Date(math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt, time.UTC),
		time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
		time.Date(math.MinInt, math.MinInt, math.MinInt, math.MinInt, math.MinInt, math.MinInt, math.MinInt, time.UTC),
		time.Date(1950, 5, 27, 0, 0, 0, 0, time.UTC),
		time.Date(1950, 5, 27, 0, 0, 0, 1, time.UTC),
		time.Date(1950, 5, 27, 1, 2, 3, 12345678, time.UTC),
		time.Date(1950, 5, 27, 13, 14, 15, 123456789, time.UTC),
		time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(100500, 5, 20, -12, -55, -8, 0, time.UTC),
		time.Date(1988, 11, 20, 12, 55, 8, 0, time.UTC),
		time.Date(100, 2, 3, 4, 5, 6, 7, time.UTC),
		time.Date(10, 2, 3, 4, 5, 6, 7, time.UTC),
		time.Date(1, 2, 3, 4, 5, 6, 7, time.UTC),
		time.Date(-100500, -10, -35, -8, -2000, -300000, -50404040, time.UTC),
	}

	const format = time.RFC3339Nano

	var (
		converterUnsafe  timestampToStringConverterUTCUnsafe
		converterDefault timestampToStringConverterUTC
	)

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.Format(format), func(t *testing.T) {
			// Check equivalence of results produced by default and unsafe converters
			expectedOut, err := converterDefault.Convert(&tc)
			require.NoError(t, err)
			actualOut, err := converterUnsafe.Convert(&tc)
			require.NoError(t, err)
			require.Equal(t, expectedOut, actualOut)
		})
	}
}

func BenchmarkTimestampToStringConverter(b *testing.B) {
	t := time.Now()

	b.Run("Default", func(b *testing.B) {
		var converter timestampToStringConverterUTC

		for i := 0; i < b.N; i++ {
			out, err := converter.Convert(&t)
			if err != nil {
				b.Fatal(err)
			}
			_ = out
		}
	})

	b.Run("Unsafe", func(b *testing.B) {
		var converter timestampToStringConverterUTCUnsafe

		for i := 0; i < b.N; i++ {
			out, err := converter.Convert(&t)
			if err != nil {
				b.Fatal(err)
			}
			_ = out
		}
	})
}
