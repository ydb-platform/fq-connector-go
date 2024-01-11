package postgresql

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
		Name: "simple",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col1",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col2",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3), ptr.Int32(4), ptr.Int32(5)},
					[]*string{
						ptr.String("pg_a"),
						ptr.String("pg_b"),
						ptr.String("pg_c"),
						ptr.String("pg_d"),
						ptr.String("pg_e"),
					},
					[]*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30), ptr.Int32(40), ptr.Int32(50)},
				},
			},
		},
	},
	"primitives": {
		Name: "primitives",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "col_01_bool",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				},
				{
					Name: "col_02_smallint",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_03_int2",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_04_smallserial",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_05_serial2",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_06_integer",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_07_int",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_08_int4",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_09_serial",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_10_serial4",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_11_bigint",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_12_int8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_13_bigserial",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_14_serial8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_15_real",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				},
				{
					Name: "col_16_float4",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				},
				{
					Name: "col_17_double_precision",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				},
				{
					Name: "col_18_float8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				},
				{
					Name: "col_19_bytea",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				},
				{
					Name: "col_20_character_n",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col_21_character_varying_n",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col_22_text",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col_23_timestamp",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				},
				{
					Name: "col_24_date",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]*uint8{ptr.Uint8(0), ptr.Uint8(1), nil},
					[]*int16{
						ptr.Int16(2),
						ptr.Int16(-2),
						nil,
					},
					[]*int16{
						ptr.Int16(3),
						ptr.Int16(-3),
						nil,
					},
					[]*int16{
						ptr.Int16(1),
						ptr.Int16(2),
						ptr.Int16(3),
					},
					[]*int16{
						ptr.Int16(1),
						ptr.Int16(2),
						ptr.Int16(3),
					},
					[]*int32{
						ptr.Int32(6),
						ptr.Int32(-6),
						nil,
					},
					[]*int32{
						ptr.Int32(7),
						ptr.Int32(-7),
						nil,
					},
					[]*int32{
						ptr.Int32(8),
						ptr.Int32(-8),
						nil,
					},
					[]*int32{
						ptr.Int32(1),
						ptr.Int32(2),
						ptr.Int32(3),
					},
					[]*int32{
						ptr.Int32(1),
						ptr.Int32(2),
						ptr.Int32(3),
					},
					[]*int64{
						ptr.Int64(11),
						ptr.Int64(-11),
						nil,
					},
					[]*int64{
						ptr.Int64(12),
						ptr.Int64(-12),
						nil,
					},
					[]*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					[]*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					[]*float32{
						ptr.Float32(15.15),
						ptr.Float32(-15.15),
						nil,
					},
					[]*float32{
						ptr.Float32(16.16),
						ptr.Float32(-16.16),
						nil,
					},
					[]*float64{
						ptr.Float64(17.17),
						ptr.Float64(-17.17),
						nil,
					},
					[]*float64{
						ptr.Float64(18.18),
						ptr.Float64(-18.18),
						nil,
					},
					[]*[]byte{
						ptr.T[[]byte]([]byte("az")),
						ptr.T[[]byte]([]byte("буки")),
						nil,
					},
					[]*string{
						ptr.String("az                  "),
						ptr.String("буки                "),
						nil,
					},
					[]*string{
						ptr.String("az"),
						ptr.String("буки"),
						nil,
					},
					[]*string{
						ptr.String("az"),
						ptr.String("буки"),
						nil,
					},
					[]*uint64{
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 456000000, time.UTC))),
						nil,
					},
					[]*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
						nil,
					},
				},
			},
		},
	},
	"datetime_format_yql": {
		Name: "datetime",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "col_01_timestamp",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				},
				{
					Name: "col_02_date",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				// In YQL mode, PG datetime values exceeding YQL date/datetime/timestamp type bounds
				// are returned as NULL
				Columns: []any{
					[]*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 456000000, time.UTC))),
					},
					[]*uint16{
						nil,
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
					},
				},
			},
		},
	},
	"datetime_format_string": {
		Name: "datetime",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "col_01_timestamp",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col_02_date",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				// In string mode, PG time values exceeding YQL date/datetime/timestamp type bounds
				// are returned without saturating them to the epoch start
				Columns: []any{
					[]*string{
						// FIXME: precision will change after YQ-2768
						ptr.String("1950-05-27T01:02:03.111Z"),
						ptr.String("1988-11-20T12:55:28.123Z"),
						ptr.String("2023-03-21T11:21:31.456Z"),
					},
					[]*string{
						ptr.String("1950-05-27"),
						ptr.String("1988-11-20"),
						ptr.String("2023-03-21"),
					},
				},
			},
		},
	},
}
