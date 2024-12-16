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

	// We used to call the unexported method *Time.date() directly before,
	// but since Go 1.23 it's restricted to use go:linkname,
	// so now we spend 3x more time here:
	year, month, day := in.Year(), in.Month(), in.Day()

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
