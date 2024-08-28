package conversion

import (
	"time"

	"github.com/ydb-platform/fq-connector-go/common"
)

type ValuePtrConverter[IN common.ValueType, OUT common.ValueType] interface {
	Convert(in *IN) (OUT, error)
}

type Collection interface {
	Bool() ValuePtrConverter[bool, uint8]
	Int8() ValuePtrConverter[int8, int8]
	Int16() ValuePtrConverter[int16, int16]
	Int32() ValuePtrConverter[int32, int32]
	Int64() ValuePtrConverter[int64, int64]
	Uint8() ValuePtrConverter[uint8, uint8]
	Uint16() ValuePtrConverter[uint16, uint16]
	Uint32() ValuePtrConverter[uint32, uint32]
	Uint64() ValuePtrConverter[uint64, uint64]
	Float32() ValuePtrConverter[float32, float32]
	Float64() ValuePtrConverter[float64, float64]
	String() ValuePtrConverter[string, string]
	StringToBytes() ValuePtrConverter[string, []byte]
	Bytes() ValuePtrConverter[[]byte, []byte]
	BytesToString() ValuePtrConverter[[]byte, string]
	Date() ValuePtrConverter[time.Time, uint16]
	DateToString() ValuePtrConverter[time.Time, string]
	Datetime() ValuePtrConverter[time.Time, uint32]
	DatetimeToString() ValuePtrConverter[time.Time, string]
	Timestamp() ValuePtrConverter[time.Time, uint64]
	TimestampToString(utc bool) ValuePtrConverter[time.Time, string]
}
