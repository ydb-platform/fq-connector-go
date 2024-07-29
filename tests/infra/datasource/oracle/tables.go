package oracle

import (
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var tables = map[string]*test_utils.Table[int64, array.Int64Builder]{
	"simple": {
		Name: "SIMPLE",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL1": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL2": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
			},
		},
		Records: []*test_utils.Record[int64, array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"COL1": []*string{
						ptr.String("oracle_a"),
						ptr.String("oracle_b"),
						ptr.String("oracle_c"),
					},
					"COL2": []*int64{
						ptr.Int64(10),
						ptr.Int64(20),
						ptr.Int64(30),
					},
				},
			},
		},
	},
	"optionals": {
		Name: "OPTIONALS",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":         common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_INT": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				// "COL_02_FLOAT": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)), TODO
				"COL_03_INT_NUMBER": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				// "COL_04_FRAC_NUMBER": TODO
				"COL_05_BINARY_FLOAT":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				"COL_06_BINARY_DOUBLE":              common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"COL_07_VARCHAR2":                   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_08_NVARCHAR2":                  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_09_CHAR_ONE":                   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_10_CHAR_SMALL":                 common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_11_NCHAR_ONE":                  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_12_NCHAR_SMALL":                common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_13_CLOB":                       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_14_NCLOB":                      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_15_RAW":                        common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"COL_16_BLOB":                       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"COL_17_DATE":                       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"COL_18_TIMESTAMP":                  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"COL_19_TIMESTAMP_W_TIMEZONE":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"COL_20_TIMESTAMP_W_LOCAL_TIMEZONE": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				// "COL_21_BFILE": TODO
				// "COL_22_JSON": TODO
			},
		},
		Records: []*test_utils.Record[int64, array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"COL_01_INT": []*int64{
						ptr.Int64(1),
						ptr.Int64(-1),
						nil,
					},
					// "COL_02_FLOAT": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)), TODO
					"COL_03_INT_NUMBER": []*int64{
						ptr.Int64(123),
						ptr.Int64(-123),
						nil,
					},
					// "COL_04_FRAC_NUMBER": TODO
					"COL_05_BINARY_FLOAT": []*float32{
						ptr.Float32(1.1),
						ptr.Float32(-1.1),
						nil,
					},
					"COL_06_BINARY_DOUBLE": []*float64{
						ptr.Float64(1.1),
						ptr.Float64(-1.1),
						nil,
					},
					"COL_07_VARCHAR2": []*string{
						ptr.String("varchar"),
						ptr.String("varchar"),
						nil,
					},
					"COL_08_NVARCHAR2": []*string{
						ptr.String("варчар"),
						ptr.String("варчар"),
						nil,
					},
					"COL_09_CHAR_ONE": []*string{
						ptr.String("c"),
						ptr.String("c"),
						nil,
					},
					"COL_10_CHAR_SMALL": []*string{
						ptr.String("cha"),
						ptr.String("cha"),
						nil,
					},
					"COL_11_NCHAR_ONE": []*string{
						ptr.String("ч"),
						ptr.String("ч"),
						nil,
					},
					"COL_12_NCHAR_SMALL": []*string{
						ptr.String("чар"),
						ptr.String("чар"),
						nil,
					},
					"COL_13_CLOB": []*string{
						ptr.String("clob"),
						ptr.String("clob"),
						nil,
					},
					"COL_14_NCLOB": []*string{
						ptr.String("клоб"),
						ptr.String("клоб"),
						nil,
					},
					"COL_15_RAW": []*[]byte{
						ptr.T([]byte("ABCD")),
						ptr.T([]byte("1234")),
						nil,
					},
					"COL_16_BLOB": []*[]byte{
						ptr.T([]byte("EF")),
						ptr.T([]byte("5678")),
						nil,
					},
					"COL_17_DATE": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 01, 01, 00, 00, 00, 000000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 01, 01, 00, 00, 00, 000000000, time.UTC))),
						nil,
					},
					"COL_18_TIMESTAMP": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 01, 01, 01, 01, 01, 111111000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 01, 01, 01, 01, 01, 111111000, time.UTC))),
						nil,
					},
					"COL_19_TIMESTAMP_W_TIMEZONE": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 01, 01, 02, 01, 01, 111111000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 01, 01, 02, 01, 01, 111111000, time.UTC))),
						nil,
					},
					"COL_20_TIMESTAMP_W_LOCAL_TIMEZONE": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 01, 01, 02, 02, 12, 111111000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 01, 01, 02, 02, 12, 111111000, time.UTC))),
						nil,
					},
				},
			},
		},
	},
	"long_table": {
		Name: "LONG_TABLE",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_LONG": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int64, array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"COL_01_LONG": []*string{
						ptr.String("long"),
						nil,
						nil,
					},
				},
			},
		},
	},
	"longraw": {
		Name: "LONGRAW",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":              common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_LONG_RAW": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
			},
		},
		Records: []*test_utils.Record[int64, array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"COL_01_LONG_RAW": []*[]byte{
						ptr.T([]byte{18}),
						nil,
						nil,
					},
				},
			},
		},
	},
	"datetime_format_yql": {
		Name: "DATETIMES",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_DATE":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				"COL_02_TIMESTAMP": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
			},
		},
		Records: []*test_utils.Record[int64, array.Int64Builder]{
			{
				// In YQL mode, PG datetime values exceeding YQL date/datetime/timestamp type bounds
				// are returned as NULL
				Columns: map[string]any{
					"ID": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"COL_01_DATE": []*uint32{
						nil,
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
					},
					"COL_02_TIMESTAMP": []*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
					},
				},
			},
		},
	},
	"datetime_format_string": {
		Name: "DATETIMES",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_DATE":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_02_TIMESTAMP": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int64, array.Int64Builder]{
			{
				// In string mode, PG time values exceeding YQL date/datetime/timestamp type bounds
				// are returned without saturating them to the epoch start
				Columns: map[string]any{
					"ID": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"COL_01_DATE": []*string{
						ptr.String("1950-05-27T01:02:03Z"),
						ptr.String("1988-11-20T12:55:28Z"),
						ptr.String("2023-03-21T11:21:31Z"),
					},
					"COL_02_TIMESTAMP": []*string{
						ptr.String("1950-05-27T01:02:03.111111Z"),
						ptr.String("1988-11-20T12:55:28.123Z"),
						ptr.String("2023-03-21T11:21:31Z"),
					},
				},
			},
		},
	},
}

// func pushdownSchemaYdb() *test_utils.TableSchema {
// 	return &test_utils.TableSchema{
// 		Columns: map[string]*Ydb.Type{
// 			"id":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
// 			"int_column":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
// 			"varchar_column": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
// 		},
// 	}
// }
