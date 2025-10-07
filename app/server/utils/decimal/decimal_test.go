package decimal

import (
	"math/big"
	"testing"
)

// Helper function to create a big.Int from a string
func bigIntFromString(s string) *big.Int {
	n := new(big.Int)
	n.SetString(s, 10)
	return n
}

func TestSerializeSpecialValues(t *testing.T) {
	// We don't have direct equivalents for NaN and Infinity in big.Int
	// But we can test nil for NaN
	result := Serialize(nil)
	if result[0] != PosNaN[0] {
		t.Errorf("Expected nil to serialize as PosNaN, got %x", result[0])
	}

	// Test values exceeding the maximum decimal
	overMax, _ := new(big.Int).SetString("100000000000000000000000000000000000", 10) // 10^35
	result = Serialize(overMax)
	if result[0] != PosInf[0] {
		t.Errorf("Expected value > MaxDecimal to serialize as PosInf, got %x", result[0])
	}

	// Test values below the minimum decimal
	underMin, _ := new(big.Int).SetString("-100000000000000000000000000000000000", 10) // -10^35
	result = Serialize(underMin)
	if result[0] != NegInf[0] {
		t.Errorf("Expected value < MinDecimal to serialize as NegInf, got %x", result[0])
	}
}

func TestSerializeZero(t *testing.T) {
	zero := big.NewInt(0)
	result := Serialize(zero)

	// Zero should be serialized with marker 0x80 (positive zero)
	if result[0] != 0x80 {
		t.Errorf("Expected zero to serialize with marker 0x80, got %x", result[0])
	}
}

func TestSerializePositiveNumbers(t *testing.T) {
	testCases := []struct {
		input    *big.Int
		expected byte // Expected marker byte
		size     int  // Expected size
	}{
		{big.NewInt(1), 0x80, 1},
		{big.NewInt(255), 0x80, 1},
		{big.NewInt(256), 0x81, 2},
		{big.NewInt(65535), 0x81, 2},
		{big.NewInt(65536), 0x82, 3},
		// Maximum value should be handled correctly
		{bigIntFromString("99999999999999999999999999999999999"), 0x8E, 15}, // 35 digits
	}

	for _, tc := range testCases {
		result := Serialize(tc.input)

		// Check marker byte
		if result[0] != tc.expected {
			t.Errorf("For %s, expected marker %x, got %x", tc.input.String(), tc.expected, result[0])
		}

		// Deserialize and check if we get the same value back
		deserialized, err := Deserialize(result)
		if err != nil {
			t.Errorf("Failed to deserialize %s: %v", tc.input.String(), err)
		}

		if deserialized.Cmp(tc.input) != 0 {
			t.Errorf("Deserialization mismatch for %s, got %s", tc.input.String(), deserialized.String())
		}
	}
}

func TestSerializeNegativeNumbers(t *testing.T) {
	testCases := []struct {
		input    *big.Int
		expected byte // Expected marker byte
		size     int  // Expected size
	}{
		{big.NewInt(-1), 0x7F, 1},
		{big.NewInt(-255), 0x7F, 1},
		{big.NewInt(-256), 0x7E, 2},
		{big.NewInt(-65535), 0x7E, 2},
		{big.NewInt(-65536), 0x7D, 3},
		// Minimum value should be handled correctly
		{bigIntFromString("-99999999999999999999999999999999999"), 0x71, 15}, // 35 digits
	}

	for _, tc := range testCases {
		result := Serialize(tc.input)

		// Check marker byte
		if result[0] != tc.expected {
			t.Errorf("For %s, expected marker %x, got %x", tc.input.String(), tc.expected, result[0])
		}

		// Deserialize and check if we get the same value back
		deserialized, err := Deserialize(result)
		if err != nil {
			t.Errorf("Failed to deserialize %s: %v", tc.input.String(), err)
		}

		if deserialized.Cmp(tc.input) != 0 {
			t.Errorf("Deserialization mismatch for %s, got %s", tc.input.String(), deserialized.String())
		}
	}
}

func TestDeserializeSpecialValues(t *testing.T) {
	testCases := []struct {
		input    [16]byte
		expected error
	}{
		{[16]byte{NegNaN[0]}, ErrNegativeNaN},
		{[16]byte{NegInf[0]}, ErrNegativeInfinity},
		{[16]byte{PosInf[0]}, ErrPositiveInfinity},
		{[16]byte{PosNaN[0]}, ErrPositiveNaN},
	}

	for _, tc := range testCases {
		_, err := Deserialize(tc.input)
		if err != tc.expected {
			t.Errorf("For special value %x, expected error %v, got %v", tc.input[0], tc.expected, err)
		}
	}
}

func TestDeserializeInvalidMarker(t *testing.T) {
	// Test with invalid marker bytes
	invalidMarkers := []byte{0x00, 0x01, 0x6F, 0x90, 0xFE, 0xFF}

	for _, marker := range invalidMarkers {
		var data [16]byte
		data[0] = marker

		_, err := Deserialize(data)
		if marker == NegNaN[0] || marker == NegInf[0] || marker == PosInf[0] || marker == PosNaN[0] {
			// These are special values, not invalid markers
			continue
		}

		if err != ErrInvalidMarker {
			t.Errorf("For invalid marker %x, expected ErrInvalidMarker, got %v", marker, err)
		}
	}
}

func TestSerializeDeserializeRoundTrip(t *testing.T) {
	// Test round-trip serialization and deserialization for various values
	testValues := []*big.Int{
		big.NewInt(0),
		big.NewInt(1),
		big.NewInt(-1),
		big.NewInt(127),
		big.NewInt(-127),
		big.NewInt(128),
		big.NewInt(-128),
		big.NewInt(255),
		big.NewInt(-255),
		big.NewInt(256),
		big.NewInt(-256),
		big.NewInt(65535),
		big.NewInt(-65535),
		big.NewInt(65536),
		big.NewInt(-65536),
		bigIntFromString("1234567890123456789012345"),
		bigIntFromString("-1234567890123456789012345"),
		// Maximum and minimum values
		MaxDecimal, // 35 digits
		MinDecimal, // 35 digits
	}

	for _, val := range testValues {
		serialized := Serialize(val)
		deserialized, err := Deserialize(serialized)

		if err != nil {
			t.Errorf("Failed to deserialize %s: %v", val.String(), err)
			continue
		}

		if deserialized.Cmp(val) != 0 {
			t.Errorf("Round-trip failed for %s, got %s", val.String(), deserialized.String())
		}
	}
}
