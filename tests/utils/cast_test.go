//nolint:unused
package utils

import "testing"

type stringLike interface {
	[]byte | string
}

func scanStringNoSwitch[IN stringLike, OUT stringLike](dest, value any) {
	**dest.(**OUT) = OUT(value.(IN))

	return
}

func scanStringSwitch1[IN stringLike, OUT stringLike](dest, value any) {
	v := OUT(value.(IN))

	switch dest := dest.(type) {
	case **OUT:
		**dest = v
	default:
		panic("incompatible type")
	}

	return
}

func scanStringSwitch2[IN stringLike, OUT stringLike](dest, value any) {
	v := OUT(value.(IN))

	switch dest := dest.(type) {
	case *OUT:
		*dest = v
	case **OUT:
		**dest = v
	default:
		panic("incompatible type")
	}

	return
}

func BenchmarkScanString(b *testing.B) {
	acceptor := new(*string)
	*acceptor = new(string)

	value := "qwerty12345"

	b.Run("no switch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			scanStringNoSwitch[string, string](acceptor, value)
		}
	})

	b.Run("switch1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			scanStringSwitch1[string, string](acceptor, value)
		}
	})

	b.Run("switch2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			scanStringSwitch1[string, string](acceptor, value)
		}
	})
}

func scanAlloc[IN stringLike, OUT stringLike](dest, value any) {
	v := OUT(value.(IN))
	*dest.(**OUT) = &v
	return
}

func scanNoAlloc[IN stringLike, OUT stringLike](dest, value any) {
	**dest.(**OUT) = OUT(value.(IN))
	return
}

func BenchmarkScanAlloc(b *testing.B) {
	value := "qwerty12345"

	b.Run("scan no alloc", func(b *testing.B) {
		acceptor := new(*string)
		*acceptor = new(string)

		for i := 0; i < b.N; i++ {
			scanNoAlloc[string, string](acceptor, value)
		}
	})

	b.Run("scan alloc", func(b *testing.B) {
		acceptor := new(*string)
		*acceptor = new(string)

		for i := 0; i < b.N; i++ {
			scanAlloc[string, string](acceptor, value)
		}
	})
}
