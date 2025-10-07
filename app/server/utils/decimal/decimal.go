// Package decimal provides utilities for working with decimal numbers.
package decimal

import (
	"errors"
	"math/big"
)

// MaxDecimal represents the maximum value for a 128-bit decimal
// 10^35 - 1 (35 nines)
var MaxDecimal, _ = new(big.Int).SetString("99999999999999999999999999999999999", 10)

// MinDecimal represents the minimum value for a 128-bit decimal
// -10^35 + 1 (negative 35 nines)
var MinDecimal, _ = new(big.Int).SetString("-99999999999999999999999999999999999", 10)

// Special values as defined in the C++ implementation
var (
	// NegNaN represents negative NaN
	NegNaN = []byte{0x00}
	// NegInf represents negative infinity
	NegInf = []byte{0x01}
	// PosInf represents positive infinity
	PosInf = []byte{0xFE}
	// PosNaN represents positive NaN
	PosNaN = []byte{0xFF}
)

// Constants for the marker byte
const (
	minMarker = 0x70
	maxMarker = 0x8F
)

// Error types for special values and invalid inputs
var (
	ErrNegativeNaN      = errors.New("negative NaN")
	ErrNegativeInfinity = errors.New("negative infinity")
	ErrPositiveInfinity = errors.New("positive infinity")
	ErrPositiveNaN      = errors.New("positive NaN")
	ErrInvalidMarker    = errors.New("invalid marker byte")
	ErrInvalidSize      = errors.New("invalid size")
)

// IsSpecialValue checks if the given byte represents a special value marker
func IsSpecialValue(b byte) bool {
	return b == NegNaN[0] || b == NegInf[0] || b == PosInf[0] || b == PosNaN[0]
}

func Serialize(value *big.Int, buf []byte) int {
	// Ensure we're working with a 128-bit value
	// Create a mask for 128 bits if needed to truncate
	var valueBytes [16]byte

	if value.Sign() >= 0 {
		// Positive number - get bytes directly
		bytes := value.Bytes()
		// Copy to valueBytes in little-endian order
		for i := 0; i < len(bytes) && i < 16; i++ {
			valueBytes[i] = bytes[len(bytes)-1-i]
		}
	} else {
		// Negative number - need two's complement in 128 bits
		// Calculate 2^128 + value (where value is negative)
		twoTo128 := new(big.Int).Lsh(big.NewInt(1), 128)
		temp := new(big.Int).Add(twoTo128, value)
		bytes := temp.Bytes()

		// Fill with 0xFF for sign extension
		for i := range valueBytes {
			valueBytes[i] = 0xFF
		}

		// Copy bytes in little-endian order
		for i := 0; i < len(bytes) && i < 16; i++ {
			valueBytes[i] = bytes[len(bytes)-1-i]
		}
	}

	size := 16
	p := 15 // index pointing to MSB

	// Check the sign bit of the second-most-significant byte
	if valueBytes[14]&0x80 != 0 {
		// Negative number - skip 0xFF bytes from MSB
		for size > 1 && p > 0 {
			p--
			if valueBytes[p] != 0xFF {
				break
			}
			size--
		}
		buf[0] = byte(0x80 - size)
	} else {
		// Positive number - skip 0x00 bytes from MSB
		for size > 1 && p > 0 {
			p--
			if valueBytes[p] != 0x00 {
				break
			}
			size--
		}
		buf[0] = byte(0x7F + size)
	}

	// Copy remaining significant bytes in big-endian order
	bufIdx := 1
	for i := 1; i < size; i++ {
		buf[bufIdx] = valueBytes[p]
		bufIdx++
		p--
	}

	return size
}
