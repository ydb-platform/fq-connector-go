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

func Serialize(
	val *decimal.Decimal,
	scale uint32,
	dst []byte, // acceptor
) {
	scaled := val.Shift(int32(scale))
	bigInt := scaled.BigInt()

	if bigInt.Sign() >= 0 {
		bigInt.FillBytes(dst)
	} else {
		tmp := new(big.Int).Lsh(big.NewInt(1), uint(blobSize*8))
		tmp.Add(tmp, bigInt)
		tmp.FillBytes(dst)
	}

	slices.Reverse(dst)
}
