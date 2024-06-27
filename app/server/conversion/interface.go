package conversion

import (
	"time"

	"github.com/ydb-platform/fq-connector-go/common"
)

type ValuePtrConverter[IN common.ValueType, OUT common.ValueType] interface {
	Convert(in *IN) (OUT, error)
}

type Collection interface {
	Bool() ValueConverter[bool, uint8]
	Int8() ValueConverter[int8, int8]
	Int16() ValueConverter[int16, int16]
	Int32() ValueConverter[int32, int32]
	Int64() ValueConverter[int64, int64]
	Uint8() ValueConverter[uint8, uint8]
	Uint16() ValueConverter[uint16, uint16]
	Uint32() ValueConverter[uint32, uint32]
	Uint64() ValueConverter[uint64, uint64]
	Float32() ValueConverter[float32, float32]
	Float64() ValueConverter[float64, float64]
	String() ValueConverter[string, string]
	StringToBytes() ValueConverter[string, []byte]
	Bytes() ValueConverter[[]byte, []byte]
	Date() ValuePtrConverter[time.Time, uint16]
	DateToString() ValuePtrConverter[time.Time, string]
	Datetime() ValuePtrConverter[time.Time, uint32]
	DatetimeToString() ValuePtrConverter[time.Time, string]
	Timestamp() ValuePtrConverter[time.Time, uint64]
	TimestampToString() ValuePtrConverter[time.Time, string]
}
