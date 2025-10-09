package decimal

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestDeserialize(t *testing.T) {
	deserializer := NewDeserializer()
	tests := []struct {
		name     string
		input    []byte
		scale    uint32
		expected string
	}{
		{
			name:     "positive small number (1)",
			input:    []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			scale:    0,
			expected: "1",
		},
		{
			name:     "positive number with two bytes (257)",
			input:    []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			scale:    0,
			expected: "257",
		},
		{
			name:     "negative number (-2)",
			input:    []byte{254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			scale:    0,
			expected: "-2",
		},
		{
			name:     "zero",
			input:    []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			scale:    0,
			expected: "0",
		},
		{
			name:     "large positive number",
			input:    []byte{255, 255, 255, 255, 255, 255, 255, 127, 0, 0, 0, 0, 0, 0, 0, 0},
			scale:    0,
			expected: "9223372036854775807", // max int64
		},
		{
			name:     "with scale - divide by 10",
			input:    []byte{206, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 1230
			scale:    1,
			expected: "123",
		},
		{
			name:     "example from task - scale 0",
			input:    []byte{174, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			scale:    0,
			expected: "2222",
		},
		{
			name:     "example from task - scale 2",
			input:    []byte{174, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			scale:    2,
			expected: "22.22",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deserializer.Deserialize(tt.input, tt.scale)

			expected, err := decimal.NewFromString(tt.expected)
			assert.NoError(t, err)
			assert.True(t, expected.Equal(*result), "Expected %s, got %s", expected.String(), result.String())
		})
	}
}

// TestDeserializeEdgeCases tests edge cases for the Deserialize function
func TestDeserializeEdgeCases(t *testing.T) {
	deserializer := NewDeserializer()

	// Test with a very large decimal number that requires big.Int
	t.Run("very large number", func(t *testing.T) {
		input := []byte{0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0}
		result := deserializer.Deserialize(input, 0)

		expected, err := decimal.NewFromString("9223372036854775808") // One more than max int64
		assert.NoError(t, err)
		assert.True(t, expected.Equal(*result), "Expected %s, got %s", expected.String(), result.String())
	})

	// Test with a very small negative number
	t.Run("very small negative number", func(t *testing.T) {
		input := []byte{255, 255, 255, 255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 255, 255, 255}
		result := deserializer.Deserialize(input, 0)

		expected, err := decimal.NewFromString("-9223372036854775809") // One less than min int64
		assert.NoError(t, err)
		assert.True(t, expected.Equal(*result), "Expected %s, got %s", expected.String(), result.String())
	})
}

func TestRoundTrip(t *testing.T) {
	serializer := NewSerializer()
	deserializer := NewDeserializer()

	tests := []struct {
		name  string
		value string
		scale uint32
	}{
		{
			name:  "positive integer",
			value: "12345",
			scale: 0,
		},
		{
			name:  "negative integer",
			value: "-98765",
			scale: 0,
		},
		{
			name:  "decimal value",
			value: "123.45",
			scale: 2,
		},
		{
			name:  "negative decimal value",
			value: "-987.65",
			scale: 2,
		},
		{
			name:  "large number",
			value: "9223372036854775807",
			scale: 0,
		},
		{
			name:  "very large number",
			value: "9223372036854775808",
			scale: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create original decimal
			original, err := decimal.NewFromString(tt.value)
			assert.NoError(t, err)

			// Serialize
			buffer := make([]byte, blobSize)
			serializer.Serialize(&original, tt.scale, buffer)

			// Deserialize
			result := deserializer.Deserialize(buffer, tt.scale)

			// Compare
			assert.True(t, original.Equal(*result), "Round trip failed: original %s, got %s", original.String(), result.String())
		})
	}
}

func BenchmarkDeserialize(b *testing.B) {
	deserializer := NewDeserializer()
	tests := []struct {
		name  string
		input []byte
		scale uint32
	}{
		{
			name:  "positive small number",
			input: []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			scale: 0,
		},
		{
			name:  "positive number with two bytes",
			input: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			scale: 0,
		},
		{
			name:  "negative number",
			input: []byte{254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			scale: 0,
		},
		{
			name:  "zero",
			input: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			scale: 0,
		},
		{
			name:  "large positive number",
			input: []byte{255, 255, 255, 255, 255, 255, 255, 127, 0, 0, 0, 0, 0, 0, 0, 0},
			scale: 0,
		},
		{
			name:  "with scale",
			input: []byte{206, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			scale: 1,
		},
		{
			name:  "very large number",
			input: []byte{0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0},
			scale: 0,
		},
		{
			name:  "very small negative number",
			input: []byte{255, 255, 255, 255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 255, 255, 255},
			scale: 0,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				deserializer.Deserialize(tt.input, tt.scale)
			}
		})
	}
}
