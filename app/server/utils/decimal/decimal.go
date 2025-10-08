// Package decimal provides utilities for working with decimal numbers.
package decimal

import (
	"math/big"
	"slices"

	"github.com/shopspring/decimal"
)

const (
	blobSize = 16
)

// Serializer provides methods for serializing decimal values with reusable big.Int objects
type Serializer struct {
	input       *big.Int
	negative    *big.Int
	twoToThe128 *big.Int
}

// NewSerializer creates a new Serializer with initialized big.Int objects
func NewSerializer() *Serializer {
	s := &Serializer{
		input:    new(big.Int),
		negative: new(big.Int),
	}

	// Pre-calculate 2^128 for negative number handling
	s.twoToThe128 = new(big.Int).Lsh(big.NewInt(1), uint(blobSize*8))

	return s
}

// Serialize converts a decimal value to a byte array representation
func (s *Serializer) Serialize(
	val *decimal.Decimal,
	scale uint32,
	dst []byte, // acceptor
) {
	// Reset the bigInt to avoid carrying over previous values
	s.input.SetInt64(0)

	// Only shift when scale > 0
	if scale > 0 {
		scaled := val.Shift(int32(scale))
		s.input.Set(scaled.BigInt())
	} else {
		// Directly use the original value's BigInt
		s.input.Set(val.BigInt())
	}

	if s.input.Sign() >= 0 {
		s.input.FillBytes(dst)
	} else {
		// For negative numbers: add 2^128 to make it positive
		s.negative.Add(s.twoToThe128, s.input)
		s.negative.FillBytes(dst)
	}

	// BigEndian -> LittleEndian
	slices.Reverse(dst)
}
