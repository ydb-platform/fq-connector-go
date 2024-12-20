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

func (collectionUnsafe) TimestampToString(utc bool) ValuePtrConverter[time.Time, string] {
	if utc {
		return timestampToStringConverterUTCUnsafe{}
	}

	return timestampToStringConverterNaive{}
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

func (timestampToStringConverterUTCUnsafe) Convert(src *time.Time) (string, error) {
	utc := src.UTC()

	buf := make([]byte, 0, 32)
	year, month, day := utc.Date()

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

	hour, minutes, seconds := utc.Clock()

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

	nanoseconds := utc.Nanosecond()
	if nanoseconds > 0 {
		buf = append(buf, byte('.'))

		buf = formatNanoseconds(buf, nanoseconds)
	}

	buf = append(buf, byte('Z'))

	p := unsafe.SliceData(buf)

	return unsafe.String(p, len(buf)), nil
}
