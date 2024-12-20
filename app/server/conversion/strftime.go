// Copyright 2020 Phus Lu. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the NOTICE file.

package conversion

const tab = "00010203040506070809" +
	"10111213141516171819" +
	"20212223242526272829" +
	"30313233343536373839" +
	"40414243444546474849" +
	"50515253545556575859" +
	"60616263646566676869" +
	"70717273747576777879" +
	"80818283848586878889" +
	"90919293949596979899"

func formatNanoseconds(buf []byte, ns int) []byte {
	// fast transformation of nanoseconds
	var tmp [9]byte
	b := ns % 100 * 2
	tmp[8] = tab[b+1]
	tmp[7] = tab[b]
	ns /= 100
	b = ns % 100 * 2
	tmp[6] = tab[b+1]
	tmp[5] = tab[b]
	ns /= 100
	b = ns % 100 * 2
	tmp[4] = tab[b+1]
	tmp[3] = tab[b]
	ns /= 100
	b = ns % 100 * 2
	tmp[2] = tab[b+1]
	tmp[1] = tab[b]
	tmp[0] = byte(ns/100) + '0'

	// check for trailing zeroes
	i := 8
	for ; i >= 0; i-- {
		if tmp[i] != '0' {
			break
		}
	}

	buf = append(buf, tmp[:i+1]...)

	return buf
}
