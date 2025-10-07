package decimal

import (
	"fmt"
	"math/big"
	"reflect"
	"testing"
)

// Helper function to create big.Int from high and low 64-bit parts
func makeInt128(high int64, low uint64) *big.Int {
	result := new(big.Int).Lsh(big.NewInt(high), 64)
	lowBig := new(big.Int).SetUint64(low)
	return result.Or(result, lowBig)
}

func TestSerialize(t *testing.T) {
	tests := []struct {
		name     string
		value    *big.Int
		expected []byte
	}{
		{
			name:     "Zero",
			value:    big.NewInt(0),
			expected: []byte{128},
		},
		{
			name:     "One",
			value:    big.NewInt(1),
			expected: []byte{129, 1},
		},
		{
			name:     "Minus one",
			value:    big.NewInt(-1),
			expected: []byte{127},
		},
		{
			name:     "127",
			value:    big.NewInt(127),
			expected: []byte{129, 127},
		},
		{
			name:     "-128",
			value:    big.NewInt(-128),
			expected: []byte{126, 128},
		},
		{
			name:     "256",
			value:    big.NewInt(256),
			expected: []byte{130, 1, 0},
		},
		{
			name:     "-256",
			value:    big.NewInt(-256),
			expected: []byte{125, 255, 0},
		},
		{
			name:     "65535",
			value:    big.NewInt(65535),
			expected: []byte{130, 255, 255},
		},
		{
			name:     "-65536",
			value:    big.NewInt(-65536),
			expected: []byte{124, 0, 0},
		},
		{
			name:     "Large positive",
			value:    makeInt128(0, 0x123456789ABCDEF0),
			expected: []byte{137, 18, 52, 86, 120, 154, 188, 222, 240},
		},
		{
			name:     "Large negative",
			value:    makeInt128(-1, 0xFEDCBA9876543210),
			expected: []byte{118, 237, 203, 169, 135, 101, 35, 18, 240},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 17) // Max size for 128-bit integer serialization
			size := Serialize(tt.value, buf)
			result := buf[:size]
			fmt.Println(">>> ", tt.value, result)

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Serialize(%s) = %v, want %v", tt.name, result, tt.expected)
			}
		})
	}
}

// Additional test to verify the buffer size returned
func TestSerializeSize(t *testing.T) {
	tests := []struct {
		name         string
		value        *big.Int
		expectedSize int
	}{
		{"Zero", big.NewInt(0), 1},
		{"One", big.NewInt(1), 2},
		{"Minus one", big.NewInt(-1), 1},
		{"127", big.NewInt(127), 2},
		{"-128", big.NewInt(-128), 2},
		{"256", big.NewInt(256), 3},
		{"-256", big.NewInt(-256), 3},
		{"65535", big.NewInt(65535), 3},
		{"-65536", big.NewInt(-65536), 3},
		{"Large positive", makeInt128(0, 0x123456789ABCDEF0), 9},
		{"Large negative", makeInt128(-1, 0xFEDCBA9876543210), 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 17)
			size := Serialize(tt.value, buf)

			if size != tt.expectedSize {
				t.Errorf("Serialize(%s) returned size %d, want %d", tt.name, size, tt.expectedSize)
			}
		})
	}
}
