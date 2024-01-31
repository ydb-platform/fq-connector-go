package utils

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/ydb-platform/fq-connector-go/common"
)

type ValueConverter[IN common.ValueType, OUT common.ValueType] interface {
	Convert(in IN) (OUT, error)
}

type ValuePtrConverter[IN common.ValueType, OUT common.ValueType] interface {
	Convert(in *IN) (OUT, error)
}

type BoolConverter struct{}

func (BoolConverter) Convert(in bool) (uint8, error) {
	// For a some reason, Bool values are converted to Arrow Uint8 rather than to Arrow native Bool.
	// See https://st.yandex-team.ru/YQL-15332 for more details.
	if in {
		return 1, nil
	}

	return 0, nil
}

type Int8Converter struct{}

func (Int8Converter) Convert(in int8) (int8, error) { return in, nil }

type Int16Converter struct{}

func (Int16Converter) Convert(in int16) (int16, error) { return in, nil }

type Int32Converter struct{}

func (Int32Converter) Convert(in int32) (int32, error) { return in, nil }

type Int64Converter struct{}

func (Int64Converter) Convert(in int64) (int64, error) { return in, nil }

type Uint8Converter struct{}

func (Uint8Converter) Convert(in uint8) (uint8, error) { return in, nil }

type Uint16Converter struct{}

func (Uint16Converter) Convert(in uint16) (uint16, error) { return in, nil }

type Uint32Converter struct{}

func (Uint32Converter) Convert(in uint32) (uint32, error) { return in, nil }

type Uint64Converter struct{}

func (Uint64Converter) Convert(in uint64) (uint64, error) { return in, nil }

type Float32Converter struct{}

func (Float32Converter) Convert(in float32) (float32, error) { return in, nil }

type Float64Converter struct{}

func (Float64Converter) Convert(in float64) (float64, error) { return in, nil }

type StringConverter struct{}

func (StringConverter) Convert(in string) (string, error) { return in, nil }

type StringToBytesConverter struct{}

func (StringToBytesConverter) Convert(in string) ([]byte, error) { return []byte(in), nil }

type BytesConverter struct{}

func (BytesConverter) Convert(in []byte) ([]byte, error) { return in, nil }

type DateConverter struct{}

func (DateConverter) Convert(in time.Time) (uint16, error) {
	out, err := common.TimeToYDBDate(&in)

	if err != nil {
		return 0, fmt.Errorf("convert time to YDB Date: %w", err)
	}

	return out, nil
}

type DateToStringConverter struct{}

func (DateToStringConverter) Convert(in *time.Time) (string, error) {
	return in.Format("2006-01-02"), nil
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

type DateToStringConverterV2 struct{}

func (DateToStringConverterV2) Convert(in *time.Time) (string, error) {
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

type DatetimeConverter struct{}

func (DatetimeConverter) Convert(in time.Time) (uint32, error) {
	out, err := common.TimeToYDBDatetime(&in)

	if err != nil {
		return 0, fmt.Errorf("convert time to YDB Datetime: %w", err)
	}

	return out, nil
}

type DatetimeToStringConverter struct{}

func (DatetimeToStringConverter) Convert(in *time.Time) (string, error) {
	return in.UTC().Format("2006-01-02T15:04:05Z"), nil
}

type TimestampConverter struct{}

func (TimestampConverter) Convert(in time.Time) (uint64, error) {
	out, err := common.TimeToYDBTimestamp(&in)

	if err != nil {
		return 0, fmt.Errorf("convert time to YDB Timestamp: %w", err)
	}

	return out, nil
}

type TimestampToStringConverter struct{}

func (TimestampToStringConverter) Convert(in *time.Time) (string, error) {
	// Using accuracy of 9 decimal places is enough for supported data sources
	// Max accuracy of date/time formats:
	// PostgreSQL - 1 microsecond (10^-6 s)
	// ClickHouse - 1 nanosecond  (10^-9 s)
	// Trailing zeros are omitted
	return in.UTC().Format("2006-01-02T15:04:05.999999999Z"), nil
}
