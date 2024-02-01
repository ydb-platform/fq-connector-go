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

//go:linkname decomposeDate time.(*Time).date
func decomposeDate(*time.Time, bool) (year int, month int, day int, dayOfYear int)

//go:linkname formatBits strconv.formatBits
func formatBits([]byte, uint64, int, bool, bool) (b []byte, s string)

type dateToStringConverterUnsafe struct{}

func (dateToStringConverterUnsafe) Convert(in *time.Time) (string, error) {
	buf := make([]byte, 0, 11)

	year, month, day, _ := decomposeDate(in, true)

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

	p := unsafe.SliceData(buf)

	return unsafe.String(p, len(buf)), nil
}
