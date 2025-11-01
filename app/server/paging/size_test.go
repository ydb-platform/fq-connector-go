package paging

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
)

type sizeFn func(any) (uint64, acceptorKind, error)

var sizeFns = map[string]sizeFn{
	"sizeOfValueReflection": sizeOfValueReflection,
	"sizeOfValueBloated":    sizeOfValueBloated,
}

type testCaseSize[Type any] struct {
	value        Type
	expectedSize uint64
	expectedKind acceptorKind
}

func (tc testCaseSize[Type]) execute(t *testing.T) {
	typeName := reflect.TypeOf(tc.value).Name()

	for fnName, fn := range sizeFns {
		fnName, fn := fnName, fn

		t.Run(fnName+"_"+typeName, func(t *testing.T) {
			x0 := tc.value
			x1 := new(Type)

			*x1 = x0

			x2 := new(*Type)

			*x2 = x1

			size0, kind0, err := fn(x0)
			require.NoError(t, err)
			require.Equal(t, size0, tc.expectedSize)
			require.Equal(t, kind0, tc.expectedKind)

			size1, kind1, err := fn(x1)
			require.NoError(t, err)
			require.Equal(t, size1, tc.expectedSize)
			require.Equal(t, kind1, tc.expectedKind)

			size2, kind2, err := fn(x2)
			require.NoError(t, err)
			require.Equal(t, size2, tc.expectedSize)
			require.Equal(t, kind2, tc.expectedKind)
		})
	}
}

func TestSize(t *testing.T) {
	type testCase interface {
		execute(t *testing.T)
	}

	testCases := []testCase{
		testCaseSize[int8]{value: 1, expectedSize: 1, expectedKind: fixedSize},
		testCaseSize[int16]{value: 1, expectedSize: 2, expectedKind: fixedSize},
		testCaseSize[int32]{value: 1, expectedSize: 4, expectedKind: fixedSize},
		testCaseSize[int64]{value: 1, expectedSize: 8, expectedKind: fixedSize},
		testCaseSize[uint8]{value: 1, expectedSize: 1, expectedKind: fixedSize},
		testCaseSize[uint16]{value: 1, expectedSize: 2, expectedKind: fixedSize},
		testCaseSize[uint32]{value: 1, expectedSize: 4, expectedKind: fixedSize},
		testCaseSize[uint64]{value: 1, expectedSize: 8, expectedKind: fixedSize},
		testCaseSize[float32]{value: 1.0, expectedSize: 4, expectedKind: fixedSize},
		testCaseSize[float64]{value: 1.0, expectedSize: 8, expectedKind: fixedSize},
		testCaseSize[string]{value: "abcde", expectedSize: 5, expectedKind: variableSize},
		testCaseSize[string]{value: "абвгд", expectedSize: 10, expectedKind: variableSize},
		testCaseSize[[]byte]{value: []byte("abcde"), expectedSize: 5, expectedKind: variableSize},
		testCaseSize[[]byte]{value: []byte("абвгд"), expectedSize: 10, expectedKind: variableSize},
		testCaseSize[time.Time]{value: time.Now().UTC(), expectedSize: 16, expectedKind: fixedSize},
	}

	for _, tc := range testCases {
		tc.execute(t)
	}
}

func BenchmarkSizeOfValue(b *testing.B) {
	for fnName, fn := range sizeFns {
		b.Run(fnName, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _, _ = fn(true)
				_, _, _ = fn(ptr.Bool(true))
				_, _, _ = fn(int64(123))
				_, _, _ = fn(ptr.Int64(123))
				_, _, _ = fn(string("abcde"))
				_, _, _ = fn(ptr.String("abcde"))
			}
		})
	}
}
