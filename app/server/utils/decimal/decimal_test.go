package decimal

import (
	"bytes"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestSerialize(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		scale    uint32
		expected []byte
	}{
		{
			name:     "positive small number (1)",
			input:    1,
			scale:    0,
			expected: []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "positive number with two bytes (257)",
			input:    257,
			scale:    0,
			expected: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "negative number (-2)",
			input:    -2,
			scale:    0,
			expected: []byte{254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
		},
		{
			name:     "zero",
			input:    0,
			scale:    0,
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "large positive number",
			input:    9223372036854775807, // max int64
			scale:    0,
			expected: []byte{255, 255, 255, 255, 255, 255, 255, 127, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "with scale - multiply by 10",
			input:    123,
			scale:    1,
			expected: []byte{206, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 123 * 10 = 1230
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create decimal from int64
			dec := decimal.NewFromInt(tt.input)

			// Create buffer to hold result
			result := make([]byte, blobSize)

			// Call Serialize
			Serialize(&dec, tt.scale, result)

			// Check result
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("Serialize() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestSerializeEdgeCases tests edge cases for the Serialize function
func TestSerializeEdgeCases(t *testing.T) {
	// Test with a very large decimal number that requires big.Int
	t.Run("very large number", func(t *testing.T) {
		// Create a large decimal that would overflow int64
		dec, err := decimal.NewFromString("9223372036854775808") // One more than max int64
		assert.NoError(t, err)

		result := make([]byte, blobSize)
		Serialize(&dec, 0, result)

		expected := []byte{0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0}
		assert.Equal(t, expected, result)
	})

	// Test with a very small negative number
	t.Run("very small negative number", func(t *testing.T) {
		dec, err := decimal.NewFromString("-9223372036854775809") // One less than min int64
		assert.NoError(t, err)

		result := make([]byte, blobSize)
		Serialize(&dec, 0, result)

		expected := []byte{255, 255, 255, 255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 255, 255, 255}
		assert.Equal(t, expected, result)
	})
}

func TestSerializeWithDecimalInput(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		scale    uint32
		expected []byte
	}{
		{
			name:     "decimal value with fraction",
			input:    "123.45",
			scale:    0,
			expected: []byte{123, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // truncated to 123
		},
		{
			name:     "decimal value with fraction and positive scale",
			input:    "123.45",
			scale:    2,
			expected: []byte{69, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 12345
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create decimal from string
			dec, err := decimal.NewFromString(tt.input)
			assert.NoError(t, err)

			// Create buffer to hold result
			result := make([]byte, blobSize)

			// Call Serialize
			Serialize(&dec, tt.scale, result)

			// Check result
			assert.Equal(t, tt.expected, result)
		})
	}
}
