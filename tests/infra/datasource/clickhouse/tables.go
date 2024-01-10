package clickhouse

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
)

var tables = map[string]*datasource.Table{
	"simple": {
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col1",
					Type: common.MakePrimitiveType(Ydb.Type_STRING),
				},
				{
					Name: "col2",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1, 2, 3, 4, 5},
					[][]byte{
						[]byte("ch_a"),
						[]byte("ch_b"),
						[]byte("ch_c"),
						[]byte("ch_d"),
						[]byte("ch_e"),
					},
					[]int32{10, 20, 30, 40, 50},
				},
			},
		},
	},
	"primitives": {
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col_01_boolean",
					Type: common.MakePrimitiveType(Ydb.Type_BOOL),
				},
				{
					Name: "col_02_int8",
					Type: common.MakePrimitiveType(Ydb.Type_INT8),
				},
				{
					Name: "col_03_uint8",
					Type: common.MakePrimitiveType(Ydb.Type_UINT8),
				},
				{
					Name: "col_04_int16",
					Type: common.MakePrimitiveType(Ydb.Type_INT16),
				},
				{
					Name: "col_05_uint16",
					Type: common.MakePrimitiveType(Ydb.Type_UINT16),
				},
				{
					Name: "col_06_int32",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col_07_uint32",
					Type: common.MakePrimitiveType(Ydb.Type_UINT32),
				},
				{
					Name: "col_08_int64",
					Type: common.MakePrimitiveType(Ydb.Type_INT64),
				},
				{
					Name: "col_09_uint64",
					Type: common.MakePrimitiveType(Ydb.Type_UINT64),
				},
				{
					Name: "col_10_float32",
					Type: common.MakePrimitiveType(Ydb.Type_FLOAT),
				},
				{
					Name: "col_11_float64",
					Type: common.MakePrimitiveType(Ydb.Type_DOUBLE),
				},
				{
					Name: "col_12_string",
					Type: common.MakePrimitiveType(Ydb.Type_STRING),
				},
				{
					Name: "col_13_string",
					Type: common.MakePrimitiveType(Ydb.Type_STRING),
				},
				{
					Name: "col_14_date",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
				{
					Name: "col_15_date32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
				{
					Name: "col_16_datetime",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				},
				{
					Name: "col_17_datetime64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1, 2},
					[]uint8{0, 1}, // []bool{false, true}
					[]int8{2, -2},
					[]uint8{3, 3},
					[]int16{4, -4},
					[]uint16{5, 5},
					[]int32{6, -6},
					[]uint32{7, 7},
					[]int64{8, -8},
					[]uint64{9, 9},
					[]float32{10.10, -10.10},
					[]float64{11.11, -11.11},
					[][]byte{[]byte("az"), []byte("буки")},
					[][]byte{
						append([]byte("az"), make([]byte, 11)...),
						append([]byte("буки"), make([]byte, 5)...),
					},
					[]*uint16{
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
					},
					[]*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
					},
					[]*uint32{
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 8, 0, time.UTC))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
					},
					[]*uint64{
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 8, 123000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 456000000, time.UTC))),
					},
				},
			},
		},
	},
	"optionals": {
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col_01_boolean",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				},
				{
					Name: "col_02_int8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT8)),
				},
				{
					Name: "col_03_uint8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT8)),
				},
				{
					Name: "col_04_int16",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_05_uint16",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT16)),
				},
				{
					Name: "col_06_int32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_07_uint32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT32)),
				},
				{
					Name: "col_08_int64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_09_uint64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT64)),
				},
				{
					Name: "col_10_float32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				},
				{
					Name: "col_11_float64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				},
				{
					Name: "col_12_string",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				},
				{
					Name: "col_13_string",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				},
				{
					Name: "col_14_date",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
				{
					Name: "col_15_date32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
				{
					Name: "col_16_datetime",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				},
				{
					Name: "col_17_datetime64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1, 2, 3},
					[]*uint8{ptr.Uint8(0), ptr.Uint8(1), nil}, // []bool{false, true, nil}
					[]*int8{ptr.Int8(2), ptr.Int8(-2), nil},
					[]*uint8{ptr.Uint8(3), ptr.Uint8(3), nil},
					[]*int16{ptr.Int16(4), ptr.Int16(-4), nil},
					[]*uint16{ptr.Uint16(5), ptr.Uint16(5), nil},
					[]*int32{ptr.Int32(6), ptr.Int32(-6), nil},
					[]*uint32{ptr.Uint32(7), ptr.Uint32(7), nil},
					[]*int64{ptr.Int64(8), ptr.Int64(-8), nil},
					[]*uint64{ptr.Uint64(9), ptr.Uint64(9), nil},
					[]*float32{ptr.Float32(10.10), ptr.Float32(-10.10), nil},
					[]*float64{ptr.Float64(11.11), ptr.Float64(-11.11), nil},
					[]*[]byte{ptr.T[[]byte]([]byte("az")), ptr.T[[]byte]([]byte("буки")), nil},
					[]*[]byte{
						ptr.T[[]byte](
							append([]byte("az"), make([]byte, 11)...),
						),
						ptr.T[[]byte](
							append([]byte("буки"), make([]byte, 5)...),
						),
						nil,
					},
					[]*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
						nil,
					},
					[]*uint16{
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
						nil,
					},
					[]*uint32{
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 8, 0, time.UTC))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
						nil,
					},
					[]*uint64{
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 8, 123000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 456000000, time.UTC))),
						nil,
					},
				},
			},
		},
	},
}
