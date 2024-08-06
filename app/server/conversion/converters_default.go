package conversion

import (
	"fmt"
	"time"

	"github.com/ydb-platform/fq-connector-go/common"
)

var _ Collection = collectionDefault{}

type collectionDefault struct{}

func (collectionDefault) Bool() ValuePtrConverter[bool, uint8]      { return boolConverter{} }
func (collectionDefault) Int8() ValuePtrConverter[int8, int8]       { return noopConverter[int8]{} }
func (collectionDefault) Int16() ValuePtrConverter[int16, int16]    { return noopConverter[int16]{} }
func (collectionDefault) Int32() ValuePtrConverter[int32, int32]    { return noopConverter[int32]{} }
func (collectionDefault) Int64() ValuePtrConverter[int64, int64]    { return noopConverter[int64]{} }
func (collectionDefault) Uint8() ValuePtrConverter[uint8, uint8]    { return noopConverter[uint8]{} }
func (collectionDefault) Uint16() ValuePtrConverter[uint16, uint16] { return noopConverter[uint16]{} }
func (collectionDefault) Uint32() ValuePtrConverter[uint32, uint32] { return noopConverter[uint32]{} }
func (collectionDefault) Uint64() ValuePtrConverter[uint64, uint64] { return noopConverter[uint64]{} }
func (collectionDefault) Float32() ValuePtrConverter[float32, float32] {
	return noopConverter[float32]{}
}
func (collectionDefault) Float64() ValuePtrConverter[float64, float64] {
	return noopConverter[float64]{}
}
func (collectionDefault) String() ValuePtrConverter[string, string] { return noopConverter[string]{} }
func (collectionDefault) StringToBytes() ValuePtrConverter[string, []byte] {
	return stringToBytesConverter{}
}
func (collectionDefault) Bytes() ValuePtrConverter[[]byte, []byte] { return noopConverter[[]byte]{} }
func (collectionDefault) BytesToString() ValuePtrConverter[[]byte, string] {
	return bytesToStringConverter{}
}
func (collectionDefault) Date() ValuePtrConverter[time.Time, uint16] { return dateConverter{} }
func (collectionDefault) DateToString() ValuePtrConverter[time.Time, string] {
	return dateToStringConverter{}
}
func (collectionDefault) Datetime() ValuePtrConverter[time.Time, uint32] { return datetimeConverter{} }
func (collectionDefault) DatetimeToString() ValuePtrConverter[time.Time, string] {
	return datetimeToStringConverter{}
}
func (collectionDefault) Timestamp() ValuePtrConverter[time.Time, uint64] {
	return timestampConverter{}
}
func (collectionDefault) TimestampToString() ValuePtrConverter[time.Time, string] {
	return timestampToStringConverter{}
}

type noopConverter[T common.ValueType] struct {
}

func (noopConverter[T]) Convert(in *T) (T, error) { return *in, nil }

type boolConverter struct{}

func (boolConverter) Convert(in *bool) (uint8, error) {
	// For a some reason, Bool values are converted to Arrow Uint8 rather than to Arrow native Bool.
	// See https://st.yandex-team.ru/YQL-15332 for more details.
	if *in {
		return 1, nil
	}

	return 0, nil
}

type stringToBytesConverter struct{}

func (stringToBytesConverter) Convert(in *string) ([]byte, error) { return []byte(*in), nil }

type dateConverter struct{}

func (dateConverter) Convert(in *time.Time) (uint16, error) {
	out, err := common.TimeToYDBDate(in)

	if err != nil {
		return 0, fmt.Errorf("convert time to YDB Date: %w", err)
	}

	return out, nil
}

type bytesToStringConverter struct{}

func (bytesToStringConverter) Convert(in *[]byte) (string, error) {
	return string((*in)[:]), nil
}

type dateToStringConverter struct{}

func (dateToStringConverter) Convert(in *time.Time) (string, error) {
	return in.Format("2006-01-02"), nil
}

type datetimeConverter struct{}

func (datetimeConverter) Convert(in *time.Time) (uint32, error) {
	out, err := common.TimeToYDBDatetime(in)

	if err != nil {
		return 0, fmt.Errorf("convert time to YDB Datetime: %w", err)
	}

	return out, nil
}

type datetimeToStringConverter struct{}

func (datetimeToStringConverter) Convert(in *time.Time) (string, error) {
	return in.UTC().Format("2006-01-02T15:04:05Z"), nil
}

type timestampConverter struct{}

func (timestampConverter) Convert(in *time.Time) (uint64, error) {
	out, err := common.TimeToYDBTimestamp(in)

	if err != nil {
		return 0, fmt.Errorf("convert time to YDB Timestamp: %w", err)
	}

	return out, nil
}

type timestampToStringConverter struct{}

func (timestampToStringConverter) Convert(in *time.Time) (string, error) {
	// Using accuracy of 9 decimal places is enough for supported data sources
	// Max accuracy of date/time formats:
	// PostgreSQL - 1 microsecond (10^-6 s)
	// ClickHouse - 1 nanosecond  (10^-9 s)
	// Oracle -  1 nanosecond  (10^-9 s)
	// Trailing zeros are omitted
	return in.UTC().Format("2006-01-02T15:04:05.999999999Z"), nil
}
