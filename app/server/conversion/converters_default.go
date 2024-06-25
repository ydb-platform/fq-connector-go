package conversion

import (
	"fmt"
	"time"

	"github.com/ydb-platform/fq-connector-go/common"
)

var _ Collection = collectionDefault{}

type collectionDefault struct{}

func (collectionDefault) Bool() ValueConverter[bool, uint8]         { return boolConverter{} }
func (collectionDefault) Int8() ValueConverter[int8, int8]          { return noopConverter[int8]{} }
func (collectionDefault) Int16() ValueConverter[int16, int16]       { return noopConverter[int16]{} }
func (collectionDefault) Int32() ValueConverter[int32, int32]       { return noopConverter[int32]{} }
func (collectionDefault) Int64() ValueConverter[int64, int64]       { return noopConverter[int64]{} }
func (collectionDefault) Uint8() ValueConverter[uint8, uint8]       { return noopConverter[uint8]{} }
func (collectionDefault) Uint16() ValueConverter[uint16, uint16]    { return noopConverter[uint16]{} }
func (collectionDefault) Uint32() ValueConverter[uint32, uint32]    { return noopConverter[uint32]{} }
func (collectionDefault) Uint64() ValueConverter[uint64, uint64]    { return noopConverter[uint64]{} }
func (collectionDefault) Float32() ValueConverter[float32, float32] { return noopConverter[float32]{} }
func (collectionDefault) Float64() ValueConverter[float64, float64] { return noopConverter[float64]{} }
func (collectionDefault) String() ValueConverter[string, string]    { return noopConverter[string]{} }
func (collectionDefault) StringToBytes() ValueConverter[string, []byte] {
	return stringToBytesConverter{}
}
func (collectionDefault) Bytes() ValueConverter[[]byte, []byte]   { return noopConverter[[]byte]{} }
func (collectionDefault) Date() ValueConverter[time.Time, uint16] { return dateConverter{} }
func (collectionDefault) DateToString() ValuePtrConverter[time.Time, string] {
	return dateToStringConverter{}
}
func (collectionDefault) Datetime() ValueConverter[time.Time, uint32] { return datetimeConverter{} }
func (collectionDefault) DatetimeToString() ValuePtrConverter[time.Time, string] {
	return datetimeToStringConverter{}
}
func (collectionDefault) Timestamp() ValueConverter[time.Time, uint64] { return timestampConverter{} }
func (collectionDefault) TimestampToString() ValuePtrConverter[time.Time, string] {
	return timestampToStringConverter{}
}

type noopConverter[T common.ValueType] struct {
}

func (noopConverter[T]) Convert(in T) (T, error) { return in, nil }

type boolConverter struct{}

func (boolConverter) Convert(in bool) (uint8, error) {
	// For a some reason, Bool values are converted to Arrow Uint8 rather than to Arrow native Bool.
	// See https://st.yandex-team.ru/YQL-15332 for more details.
	if in {
		return 1, nil
	}

	return 0, nil
}

type stringToBytesConverter struct{}

func (stringToBytesConverter) Convert(in string) ([]byte, error) { return []byte(in), nil }

type dateConverter struct{}

func (dateConverter) Convert(in time.Time) (uint16, error) {
	out, err := common.TimeToYDBDate(&in)

	if err != nil {
		return 0, fmt.Errorf("convert time to YDB Date: %w", err)
	}

	return out, nil
}

type dateToStringConverter struct{}

func (dateToStringConverter) Convert(in *time.Time) (string, error) {
	return in.Format("2006-01-02"), nil
}

type datetimeConverter struct{}

func (datetimeConverter) Convert(in time.Time) (uint32, error) {
	out, err := common.TimeToYDBDatetime(&in)

	if err != nil {
		return 0, fmt.Errorf("convert time to YDB Datetime: %w", err)
	}

	return out, nil
}

type datetimeToStringConverter struct{}

func (datetimeToStringConverter) Convert(in *time.Time) (string, error) {
	return in.Format("2006-01-02T15:04:05Z"), nil
}

type timestampConverter struct{}

func (timestampConverter) Convert(in time.Time) (uint64, error) {
	out, err := common.TimeToYDBTimestamp(&in)

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
	// Trailing zeros are omitted
	return in.Format("2006-01-02T15:04:05.999999999Z"), nil
}
