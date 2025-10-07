// Package decimal provides utilities for working with decimal numbers.
package decimal

import (
	"math/big"

	"github.com/shopspring/decimal"
)

const (
	blobSize = 16
)

func Serialize(
	val *decimal.Decimal,
	scale uint32,
	dst []byte, // acceptor
) {
	scaled := val.Shift(int32(scale))

	if !scaled.IsInteger() {
		scaled = scaled.Truncate(0)
	}

	bigInt := scaled.BigInt()

	if bigInt.Sign() >= 0 {
		bigInt.FillBytes(dst)
	} else {
		tmp := new(big.Int).Lsh(big.NewInt(1), uint(blobSize*8))
		tmp.Add(tmp, bigInt)
		tmp.FillBytes(dst)
	}

	// Reverse bytes to convert from big-endian to little-endian
	for i := 0; i < blobSize/2; i++ {
		dst[i], dst[blobSize-1-i] = dst[blobSize-1-i], dst[i]
	}
}
