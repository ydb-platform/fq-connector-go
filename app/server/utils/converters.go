package utils

import (
	"fmt"
	"time"

	"github.com/ydb-platform/fq-connector-go/common"
)

type ValueConverter[IN common.ValueType, OUT common.ValueType] interface {
	Convert(in IN) (OUT, error)
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

func (DateToStringConverter) Convert(in time.Time) (string, error) {
	return in.Format("2006-01-02"), nil
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

func (DatetimeToStringConverter) Convert(in time.Time) (string, error) {
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

func (TimestampToStringConverter) Convert(in time.Time) (string, error) {
	// Using accuracy of 9 decimal places is enough for supported data sources
	// Max accuracy of date/time formats:
	// PostgreSQL - 1 microsecond (10^-6 s)
	// ClickHouse - 1 nanosecond  (10^-9 s)
	// Trailing zeros are omitted
	return in.UTC().Format("2006-01-02T15:04:05.999999999Z"), nil
}
