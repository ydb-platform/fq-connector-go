// Package decimal provides utilities for working with decimal numbers.
package decimal

import (
	"math/big"

	"github.com/apache/arrow/go/v13/arrow/array"
)

// Упаковка big.Int в Arrow Decimal128 формат
func PackDecimalToArrow(value *big.Int, builder *array.FixedSizeBinaryBuilder) {
	result := make([]byte, 16)
	copy(result, value.Bytes())
	builder.Append(result)
}
