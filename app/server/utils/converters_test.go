package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDateToStringConverter(t *testing.T) {
	type testCase struct {
		in time.Time
	}

	testCases := []testCase{
		{
			in: time.Date(1950, 5, 27, 0, 0, 0, 0, time.UTC),
		},
		{
			in: time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	const format = "2006-01-02"

	var converter DateToStringConverterV2

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.in.Format(format), func(t *testing.T) {
			actual, err := converter.Convert(&tc.in)
			require.NoError(t, err)
			// check equivalence
			require.Equal(t, tc.in.Format(format), actual)
		})
	}
}

func BenchmarkDateToStringConverter(b *testing.B) {
	t := time.Now()

	b.Run("V1", func(b *testing.B) {
		var converter DateToStringConverter

		for i := 0; i < b.N; i++ {
			out, err := converter.Convert(&t)
			if err != nil {
				b.Fatal(err)
			}
			_ = out
		}
	})

	b.Run("V2", func(b *testing.B) {
		var converter DateToStringConverterV2

		for i := 0; i < b.N; i++ {
			out, err := converter.Convert(&t)
			if err != nil {
				b.Fatal(err)
			}
			_ = out
		}
	})
}
