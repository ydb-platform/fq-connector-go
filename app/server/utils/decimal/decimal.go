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

// Serialize converts a big.Int to a binary representation compatible with YQL Decimal format.
// Returns a fixed-size [16]byte array.
func Serialize(value *big.Int) [16]byte {
	var result [16]byte

	// Handle nil input
	if value == nil {
		result[0] = PosNaN[0]
		return result
	}

	// Check for special values
	// In Go, we don't have direct equivalents for NaN and Infinity in big.Int
	// But we can check if the value exceeds the max/min decimal range
	if value.Cmp(MaxDecimal) > 0 {
		result[0] = PosInf[0]
		return result
	}
	if value.Cmp(MinDecimal) < 0 {
		result[0] = NegInf[0]
		return result
	}

	// Get the bytes of the big.Int in big-endian format
	bytes := value.Bytes()
	isNegative := value.Sign() < 0

	// If the value is zero, we need special handling
	if len(bytes) == 0 {
		if isNegative {
			result[0] = 0x80 // Negative zero
		} else {
			result[0] = 0x7F + 1 // Positive zero with 1 byte size
		}
		return result
	}

	// For negative numbers, we need to handle the two's complement representation
	if isNegative {
		// Create a new big.Int with the absolute value
		absValue := new(big.Int).Abs(value)
		// Get the bytes of the absolute value
		bytes = absValue.Bytes()
	}

	// Calculate the size needed
	size := len(bytes)
	if size > 15 {
		size = 15 // Maximum size is 15 bytes plus marker
	}

	// Set the marker byte based on sign and size
	if isNegative {
		result[0] = byte(0x80 - size)
	} else {
		result[0] = byte(0x7F + size)
	}

	// Copy the bytes in reverse order (to match the C++ implementation)
	// The C++ code copies from the end of the value to the beginning
	for i := 0; i < size; i++ {
		if i < len(bytes) {
			result[i+1] = bytes[len(bytes)-1-i]
		}
	}

	return result
}

// Deserialize converts a binary representation in YQL Decimal format to a big.Int.
// It takes a 16-byte array and returns the deserialized big.Int value.
// If the input represents a special value (NaN, Infinity), it returns nil and an error.
func Deserialize(data [16]byte) (*big.Int, error) {
	// Check for special values
	if IsSpecialValue(data[0]) {
		switch data[0] {
		case NegNaN[0]:
			return nil, ErrNegativeNaN
		case NegInf[0]:
			return nil, ErrNegativeInfinity
		case PosInf[0]:
			return nil, ErrPositiveInfinity
		case PosNaN[0]:
			return nil, ErrPositiveNaN
		}
	}

	// Get the marker byte
	marker := data[0]

	// Check if the marker is valid
	if marker < minMarker || marker > maxMarker {
		return nil, ErrInvalidMarker
	}

	// Determine if the value is negative and calculate the size
	isNegative := marker < 0x80
	var size int
	if isNegative {
		size = int(0x80 - marker)
	} else {
		size = int(marker - 0x7F)
	}

	// Check if the size is valid
	if size < 1 || size > 15 {
		return nil, ErrInvalidSize
	}

	// Create a buffer to hold the bytes in big-endian order
	buf := make([]byte, size)

	// Copy the bytes in reverse order (to match the C++ implementation)
	for i := 0; i < size; i++ {
		buf[size-1-i] = data[i+1]
	}

	// Create a new big.Int from the bytes
	result := new(big.Int).SetBytes(buf)

	// Set the sign if negative
	if isNegative {
		result.Neg(result)
	}

	// Check if the value is within the valid range
	if result.Cmp(MaxDecimal) > 0 {
		return nil, ErrPositiveInfinity
	}
	if result.Cmp(MinDecimal) < 0 {
		return nil, ErrNegativeInfinity
	}

	return result, nil
}
