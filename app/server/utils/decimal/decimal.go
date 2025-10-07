// Package decimal provides utilities for working with decimal numbers.
package decimal

import (
	"math/big"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/shopspring/decimal"
)

const blobSize = 16

// func DecimalToTwosComplementScaled(
// 	val *decimal.Decimal,
// 	scale int32,
// 	builder *array.FixedSizeBinaryBuilder) {
// 	// Scale the decimal to convert fractional part to integer
// 	// e.g., with scale=2, 1.23 becomes 123
// 	scaled := val.Shift(scale) // Multiply by 10^scale

// 	if !scaled.IsInteger() {
// 		scaled = scaled.Truncate(0)
// 	}

// 	bigInt := scaled.BigInt()

// 	bytes := make([]byte, blobSize)

// 	if bigInt.Sign() >= 0 {
// 		copy(bytes, bigInt.Bytes())
// 	} else {
// 		fmt.Println("CRAB 1 ", val.BigInt().Bytes())
// 		tmp := new(big.Int).Lsh(big.NewInt(1), uint(blobSize*8))
// 		tmp.Add(tmp, bigInt)
// 		copy(bytes, tmp.Bytes())
// 		fmt.Println("CRAB 2 ", bytes)
// 	}

// 	builder.Append(bytes)
// }

func DecimalToTwosComplementScaled(
	val *decimal.Decimal,
	scale int32,
	builder *array.FixedSizeBinaryBuilder) {
	scaled := val.Shift(scale)

	if !scaled.IsInteger() {
		scaled = scaled.Truncate(0)
	}

	bigInt := scaled.BigInt()
	bytes := make([]byte, blobSize)

	if bigInt.Sign() >= 0 {
		bigInt.FillBytes(bytes)
	} else {
		tmp := new(big.Int).Lsh(big.NewInt(1), uint(blobSize*8))
		tmp.Add(tmp, bigInt)
		tmp.FillBytes(bytes)
	}

	// Reverse bytes to convert from big-endian to little-endian
	for i := 0; i < blobSize/2; i++ {
		bytes[i], bytes[blobSize-1-i] = bytes[blobSize-1-i], bytes[i]
	}

	builder.Append(bytes)
}
