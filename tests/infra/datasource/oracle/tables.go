package oracle

import (
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

var tables = map[string]*test_utils.Table[int64, *array.Int64Builder]{
	"simple": {
		Name:                  "SIMPLE",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL1": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL2": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
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
	"primitives": {
		Name:                  "PRIMITIVES",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"COL_00_ID":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_INT": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				// "COL_02_FLOAT": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)), TODO
				"COL_03_INT_NUMBER": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				// "COL_04_FRAC_NUMBER": TODO
				// "COL_05_BINARY_FLOAT":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)), TODO go-ora bug
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
				"COL_17_DATE":                       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				"COL_18_TIMESTAMP":                  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"COL_19_TIMESTAMP_W_TIMEZONE":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"COL_20_TIMESTAMP_W_LOCAL_TIMEZONE": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"COL_21_JSON":                       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_JSON)),
				// "COL_21_BFILE": TODO
				// "COL_22_JSON": TODO
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"COL_00_ID": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"COL_01_INT": []*int64{
						ptr.Int64(1),
						nil,
						ptr.Int64(-1),
					},
					// "COL_02_FLOAT": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)), TODO
					"COL_03_INT_NUMBER": []*int64{
						ptr.Int64(123),
						nil,
						ptr.Int64(-123),
					},
					// "COL_04_FRAC_NUMBER": TODO
					// "COL_05_BINARY_FLOAT": []*float32{
					// 	ptr.Float32(1.1),
					// 	ptr.Float32(-1.1),
					// 	nil,
					// },
					"COL_06_BINARY_DOUBLE": []*float64{
						ptr.Float64(1.1),
						nil,
						ptr.Float64(-1.1),
					},
					"COL_07_VARCHAR2": []*string{
						ptr.String("varchar"),
						nil,
						ptr.String("varchar"),
					},
					"COL_08_NVARCHAR2": []*string{
						ptr.String("варчар"),
						nil,
						ptr.String("варчар"),
					},
					"COL_09_CHAR_ONE": []*string{
						ptr.String("c"),
						nil,
						ptr.String("c"),
					},
					"COL_10_CHAR_SMALL": []*string{
						ptr.String("cha"),
						nil,
						ptr.String("cha"),
					},
					"COL_11_NCHAR_ONE": []*string{
						ptr.String("ч"),
						nil,
						ptr.String("ч"),
					},
					"COL_12_NCHAR_SMALL": []*string{
						ptr.String("чар"),
						nil,
						ptr.String("чар"),
					},
					"COL_13_CLOB": []*string{
						ptr.String("clob"),
						nil,
						ptr.String("clob"),
					},
					"COL_14_NCLOB": []*string{
						ptr.String("клоб"),
						nil,
						ptr.String("клоб"),
					},
					"COL_15_RAW": []*[]byte{
						ptr.T([]byte("ABCD")),
						nil,
						ptr.T([]byte("1234")),
					},
					"COL_16_BLOB": []*[]byte{
						ptr.T([]byte("EF")),
						nil,
						ptr.T([]byte("5678")),
					},
					"COL_17_DATE": []*uint32{
						ptr.Uint32(common.MustTimeToYDBType(common.TimeToYDBDatetime,
							time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))),
						nil,
						ptr.Uint32(common.MustTimeToYDBType(common.TimeToYDBDatetime,
							time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))),
					},
					"COL_18_TIMESTAMP": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 1, 1, 1, 1, 1, 111111000, time.UTC))),
						nil,
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 1, 1, 1, 1, 1, 111111000, time.UTC))),
					},
					"COL_19_TIMESTAMP_W_TIMEZONE": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 1, 1, 2, 1, 1, 111111000, time.UTC))),
						nil,
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 1, 1, 2, 1, 1, 111111000, time.UTC))),
					},
					"COL_20_TIMESTAMP_W_LOCAL_TIMEZONE": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 1, 1, 2, 12, 1, 111111000, time.UTC))),
						nil,
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1970, 1, 1, 2, 12, 1, 111111000, time.UTC))),
					},
					"COL_21_JSON": []*string{
						ptr.String("{\"friends\":" +
							"[{\"name\":\"James Holden\",\"age\":35}," +
							"{\"name\":\"Naomi Nagata\",\"age\":30}]}"),
						nil,
						ptr.String("{\"TODO\":\"unicode\"}"),
					},
				},
			},
		},
	},
	"long_table": {
		Name:                  "LONG_TABLE",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_LONG": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
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
		Name:                  "LONGRAW",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":              common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_LONG_RAW": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"COL_01_LONG_RAW": []*[]byte{
						ptr.T([]byte("12")),
						nil,
						nil,
					},
				},
			},
		},
	},
	"datetime_format_yql": {
		Name:                  "DATETIMES",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_DATE":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				"COL_02_TIMESTAMP": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
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
							common.TimeToYDBDatetime, time.Date(2023, 3, 21, 11, 21, 31, 0, time.UTC))),
					},
					"COL_02_TIMESTAMP": []*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 3, 21, 11, 21, 31, 0, time.UTC))),
					},
				},
			},
		},
	},
	"datetime_format_string": {
		Name:                  "DATETIMES",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"ID":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_DATE":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_02_TIMESTAMP": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
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
	"timestamps_format_yql": {
		Name:                  "TIMESTAMPS",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				// oracle rounds on insert if data more precise than column
				"COL_00_ID":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_TIMESTAMP_0": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"COL_02_TIMESTAMP_1": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"COL_03_TIMESTAMP_6": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"COL_04_TIMESTAMP_7": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"COL_05_TIMESTAMP_9": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				// In YQL mode, PG datetime values exceeding YQL date/datetime/timestamp type bounds
				// are returned as NULL
				Columns: map[string]any{
					"COL_00_ID": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"COL_01_TIMESTAMP_0": []*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 3, 21, 11, 21, 32, 0, time.UTC))),
					},
					"COL_02_TIMESTAMP_1": []*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 100000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 3, 21, 11, 21, 31, 900000000, time.UTC))),
					},
					"COL_03_TIMESTAMP_6": []*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123123000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 3, 21, 11, 21, 31, 888889000, time.UTC))),
					},
					"COL_04_TIMESTAMP_7": []*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123123100, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 3, 21, 11, 21, 31, 888888900, time.UTC))),
					},
					"COL_05_TIMESTAMP_9": []*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123123123, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 3, 21, 11, 21, 31, 888888888, time.UTC))),
					},
				},
			},
		},
	},
	"timestamps_format_string": {
		Name:                  "TIMESTAMPS",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				// oracle rounds on insert if data more precise than column
				"COL_00_ID":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"COL_01_TIMESTAMP_0": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_02_TIMESTAMP_1": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_03_TIMESTAMP_6": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_04_TIMESTAMP_7": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"COL_05_TIMESTAMP_9": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				// In string mode, PG time values exceeding YQL date/datetime/timestamp type bounds
				// are returned without saturating them to the epoch start
				Columns: map[string]any{
					"COL_00_ID": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"COL_01_TIMESTAMP_0": []*string{
						ptr.String("1950-05-27T01:02:03Z"),
						ptr.String("1988-11-20T12:55:28Z"),
						ptr.String("2023-03-21T11:21:32Z"),
					},
					"COL_02_TIMESTAMP_1": []*string{
						ptr.String("1950-05-27T01:02:03.1Z"),
						ptr.String("1988-11-20T12:55:28.1Z"),
						ptr.String("2023-03-21T11:21:31.9Z"),
					},
					"COL_03_TIMESTAMP_6": []*string{
						ptr.String("1950-05-27T01:02:03.111111Z"),
						ptr.String("1988-11-20T12:55:28.123123Z"),
						ptr.String("2023-03-21T11:21:31.888889Z"),
					},
					"COL_04_TIMESTAMP_7": []*string{
						ptr.String("1950-05-27T01:02:03.1111111Z"),
						ptr.String("1988-11-20T12:55:28.1231231Z"),
						ptr.String("2023-03-21T11:21:31.8888889Z"),
					},
					"COL_05_TIMESTAMP_9": []*string{
						ptr.String("1950-05-27T01:02:03.111111111Z"),
						ptr.String("1988-11-20T12:55:28.123123123Z"),
						ptr.String("2023-03-21T11:21:31.888888888Z"),
					},
				},
			},
		},
	},
	"pushdown_comparison_L": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(1)},
					"INT_COLUMN":     []*int64{ptr.Int64(10)},
					"VARCHAR_COLUMN": []*string{ptr.T("a")},
				},
			},
		},
	},
	"pushdown_comparison_LE": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(1), ptr.Int64(2)},
					"INT_COLUMN":     []*int64{ptr.Int64(10), ptr.Int64(20)},
					"VARCHAR_COLUMN": []*string{ptr.T("a"), ptr.T("b")},
				},
			},
		},
	},
	"pushdown_comparison_EQ": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(2)},
					"INT_COLUMN":     []*int64{ptr.Int64(20)},
					"VARCHAR_COLUMN": []*string{ptr.T("b")},
				},
			},
		},
	},
	"pushdown_comparison_GE": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(2), ptr.Int64(3)},
					"INT_COLUMN":     []*int64{ptr.Int64(20), ptr.Int64(30)},
					"VARCHAR_COLUMN": []*string{ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_comparison_G": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(1), ptr.Int64(2), ptr.Int64(3)},
					"INT_COLUMN":     []*int64{ptr.Int64(10), ptr.Int64(20), ptr.Int64(30)},
					"VARCHAR_COLUMN": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_comparison_NE": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(2), ptr.Int64(3), ptr.Int64(4)},
					"INT_COLUMN":     []*int64{ptr.Int64(20), ptr.Int64(30), nil},
					"VARCHAR_COLUMN": []*string{ptr.T("b"), ptr.T("c"), nil},
				},
			},
		},
	},
	"pushdown_comparison_NULL": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(4)},
					"INT_COLUMN":     []*int64{nil},
					"VARCHAR_COLUMN": []*string{nil},
				},
			},
		},
	},
	"pushdown_comparison_NOT_NULL": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(1), ptr.Int64(2), ptr.Int64(3)},
					"INT_COLUMN":     []*int64{ptr.Int64(10), ptr.Int64(20), ptr.Int64(30)},
					"VARCHAR_COLUMN": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_BETWEEN": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(1), ptr.Int64(2), ptr.Int64(3)},
					"INT_COLUMN":     []*int64{ptr.Int64(10), ptr.Int64(20), ptr.Int64(30)},
					"VARCHAR_COLUMN": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_NOT_BETWEEN": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(1)},
					"INT_COLUMN":     []*int64{ptr.Int64(10)},
					"VARCHAR_COLUMN": []*string{ptr.T("a")},
				},
			},
		},
	},
	"pushdown_conjunction": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(2), ptr.Int64(3)},
					"INT_COLUMN":     []*int64{ptr.Int64(20), ptr.Int64(30)},
					"VARCHAR_COLUMN": []*string{ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_disjunction": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(1), ptr.Int64(2), ptr.Int64(3)},
					"INT_COLUMN":     []*int64{ptr.Int64(10), ptr.Int64(20), ptr.Int64(30)},
					"VARCHAR_COLUMN": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_negation": {
		Name:                  "PUSHDOWN",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"ID":             []*int64{ptr.Int64(4)},
					"INT_COLUMN":     []*int64{nil},
					"VARCHAR_COLUMN": []*string{nil},
				},
			},
		},
	},
}

func pushdownSchemaYdb() *test_utils.TableSchema {
	return &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			"ID":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
			"INT_COLUMN":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
			"VARCHAR_COLUMN": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
		},
	}
}

func newInt64IDArrayBuilder(pool memory.Allocator) func() *array.Int64Builder {
	return func() *array.Int64Builder {
		return array.NewInt64Builder(pool)
	}
}
