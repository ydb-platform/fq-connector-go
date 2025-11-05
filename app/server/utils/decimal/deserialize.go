// Package decimal provides utilities for working with decimal numbers.
package decimal

import (
	"math/big"
	"slices"

	"github.com/shopspring/decimal"
)

// Deserialize converts a byte array representation to a decimal value
func Deserialize(
	src []byte, // source byte array
	scale uint32, // scale factor
) *decimal.Decimal {
	// Make a copy of the source to avoid modifying the original
	buf := make([]byte, len(src))
	copy(buf, src)

	// LittleEndian -> BigEndian
	slices.Reverse(buf)

	// Create a new big.Int from the bytes
	bigInt := new(big.Int).SetBytes(buf)

	// Check if the number is negative (most significant bit is set)
	isNegative := len(buf) > 0 && (buf[0]&0x80) != 0

	if isNegative {
		// For negative numbers: subtract from 2^{8*blobSize} to get the original negative value
		twoToThe128 := new(big.Int).Lsh(big.NewInt(1), uint(blobSize*8))

		bigInt = new(big.Int).Sub(twoToThe128, bigInt)
		bigInt.Neg(bigInt)
	}

	// Create decimal from big.Int
	result := decimal.NewFromBigInt(bigInt, 0)

	// Only shift when scale > 0
	if scale > 0 {
		result = result.Shift(-int32(scale))
	}

	return &result
}
