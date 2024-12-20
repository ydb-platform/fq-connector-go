package conversion

import (
	"time"
	"unsafe"
)

var _ Collection = collectionUnsafe{}

type collectionUnsafe struct {
	collectionDefault
}

func (collectionUnsafe) DateToString() ValuePtrConverter[time.Time, string] {
	return dateToStringConverterUnsafe{}
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}

	return x
}

type dateToStringConverterUnsafe struct{}

func (dateToStringConverterUnsafe) Convert(in *time.Time) (string, error) {
	buf := make([]byte, 0, 11)
	year, month, day := in.Date()

	if year < 0 {
		buf = append(buf, byte('-'))
	}

	absYear := absInt(year)

	switch {
	case absYear < 10:
		buf = append(buf, []byte("000")...)
	case absYear < 100:
		buf = append(buf, []byte("00")...)
	case absYear < 1000:
		buf = append(buf, byte('0'))
	}

	buf, _ = formatBits(buf, uint64(absYear), 10, false, true)

	// month

	buf = append(buf, byte('-'))
	if month < 10 {
		buf = append(buf, byte('0'))
	}

	buf, _ = formatBits(buf, uint64(month), 10, false, true)

	// day

	buf = append(buf, byte('-'))
	if day < 10 {
		buf = append(buf, byte('0'))
	}

	buf, _ = formatBits(buf, uint64(day), 10, false, true)

	p := unsafe.SliceData(buf)

	return unsafe.String(p, len(buf)), nil
}

type timestampToStringConverterUTCUnsafe struct{}

func (timestampToStringConverterUTCUnsafe) Convert(in *time.Time) (string, error) {
	buf := make([]byte, 0, 32)
	year, month, day := in.Date()

	// year

	if year < 0 {
		buf = append(buf, byte('-'))
	}

	absYear := absInt(year)

	switch {
	case absYear < 10:
		buf = append(buf, []byte("000")...)
	case absYear < 100:
		buf = append(buf, []byte("00")...)
	case absYear < 1000:
		buf = append(buf, byte('0'))
	}

	buf, _ = formatBits(buf, uint64(absYear), 10, false, true)

	// month

	buf = append(buf, byte('-'))
	if month < 10 {
		buf = append(buf, byte('0'))
	}

	buf, _ = formatBits(buf, uint64(month), 10, false, true)

	// day

	buf = append(buf, byte('-'))
	if day < 10 {
		buf = append(buf, byte('0'))
	}

	buf, _ = formatBits(buf, uint64(day), 10, false, true)

	// T
	buf = append(buf, byte('T'))

	hour, minutes, seconds := in.Clock()

	// hours

	if hour < 10 {
		buf = append(buf, byte('0'))
	}

	buf, _ = formatBits(buf, uint64(hour), 10, false, true)

	buf = append(buf, byte(':'))

	// minutes

	if minutes < 10 {
		buf = append(buf, byte('0'))
	}

	buf, _ = formatBits(buf, uint64(minutes), 10, false, true)

	buf = append(buf, byte(':'))

	// seconds

	if seconds < 10 {
		buf = append(buf, byte('0'))
	}

	buf, _ = formatBits(buf, uint64(seconds), 10, false, true)

	// nanoseconds

	nanoseconds := in.Nanosecond()
	if nanoseconds > 0 {
		buf = append(buf, byte('.'))

		buf = formatNanoseconds(buf, nanoseconds)
	}

	buf = append(buf, byte('Z'))

	p := unsafe.SliceData(buf)

	return unsafe.String(p, len(buf)), nil
}

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

func digitsInNumber(n int) int {
	if n < 0 {
		n = -n
	}

	if n == 0 {
		return 1
	}

	digits := 0

	for n > 0 {
		digits++
		n /= 10
	}

	return digits
}
