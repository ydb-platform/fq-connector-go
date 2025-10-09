// Package decimal provides utilities for working with decimal numbers.
package decimal

import (
	"math/big"
	"slices"

	"github.com/shopspring/decimal"
)

// Deserializer provides methods for deserializing byte arrays to decimal values with reusable big.Int objects
type Deserializer struct {
	output      *big.Int
	negative    *big.Int
	twoToThe128 *big.Int
}

// NewDeserializer creates a new Deserializer with initialized big.Int objects
func NewDeserializer() *Deserializer {
	d := &Deserializer{
		output:   new(big.Int),
		negative: new(big.Int),
	}

	// Pre-calculate 2^128 for negative number handling
	d.twoToThe128 = new(big.Int).Lsh(big.NewInt(1), uint(blobSize*8))

	return d
}

// Deserialize converts a byte array representation to a decimal value
func (d *Deserializer) Deserialize(
	src []byte, // source byte array
	scale uint32, // scale factor
) *decimal.Decimal {
	// Make a copy of the source to avoid modifying the original
	buf := make([]byte, len(src))
	copy(buf, src)

	// LittleEndian -> BigEndian
	slices.Reverse(buf)

	// Reset the bigInt to avoid carrying over previous values
	d.output.SetBytes(buf)

	// Check if the number is negative (most significant bit is set)
	isNegative := len(buf) > 0 && (buf[0]&0x80) != 0

	if isNegative {
		// For negative numbers: subtract from 2^128 to get the original negative value
		d.negative.Sub(d.twoToThe128, d.output)
		d.output.Set(d.negative)
		d.output.Neg(d.output)
	}

	// Create decimal from big.Int
	result := decimal.NewFromBigInt(d.output, 0)

	// Only shift when scale > 0
	if scale > 0 {
		result = result.Shift(-int32(scale))
	}

	return &result
}
