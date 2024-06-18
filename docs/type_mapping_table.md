# Сопоставление систем типов

Одной из ключевых функций коннектора является представление метаданных и данных внешних источников в формате, понятном для YDB. При извлечении метаданных для этого используется система типов языка [`YQL`](https://ydb.tech/docs/ru/yql/reference/types/), при извлечении данных - система типов [`Apache Arrow`](https://arrow.apache.org/docs/cpp/api/datatype.html). В данном упомянутые системы сопоставляются с системами типов различных внешних источников. Актуальная реализация коннектора должна соответствовать этой таблице.

## О различии систем типов разных источников данных

Фундаментальной особенностью системы типов любой реляционных СУБД является отношение к опциональности значений в столбцах. Может ли колонка содержать значение `NULL` или нет? По этому критерию все внешние источники данных можно разделить на две группы:

* С системой типов, чётко разделяющей nullable и не-nullable колонки. Пример - `ClickHouse`. По умолчанию все колонки в таблицах `ClickHouse` non-nullable, то есть пользователь физически не может записать в них `NULL`. Однако он может явно указать колонку как `NULLABLE` в момент создания таблицы и заплатить за это более высокими накладными расходами при хранении данных. Ещё один пример - `YDB`: там примитивные типы данных [не могут](https://ydb.tech/docs/ru/yql/reference/types/optional) хранить значения `NULL`.
* С системой типов, где все колонки потенциально nullable. Пример - `PostgreSQL`. Даже если колонка была создана c `NOT NULL` constraint, всё равно физически она может содержать `NULL`ы. 

При формировании схемы таблицы в момент отдачи метаданных (метод `DescribeTable`) для описания non-nullable колонок должны использоваться обычные типы данных, например `INT8`, `STRING`, а для nullable колонок - [опциональные](https://ydb.tech/docs/ru/yql/reference/types/optional), то есть `Optional<INT8>`, `Optional<STRING>`. 

## Таблица соответствия типов

:one: - система типов с nullable и non-nullable типами.
:two: - система типов только с nullable типами. 
:white_check_mark: - тип поддерживается
:x: - тип не поддерживается

| :one: YDB / YQL | Arrow | Go | :one: ClickHouse | :two: PostgreSQL (15) / Greenplum (6)| :two: MySQL | MS SQL Server |
| --- | ----- | -- | ---------- | ---------- | ----- | ------ |
| `BOOL` | `UINT8` | `bool` | :white_check_mark: `Bool` | :white_check_mark: `boolean`, `bool` (1 байт) | :white_check_mark: `bool` (`tinyint(1)`) | `BIT` |
| - | - | - | - | :x: `bit[(n)]`, `bit varying[(n)]`, `varbit` (битовые строки, состоящие из 0 и 1)| :x: `bit(n)` (bit-valued literals, до 64 бит) | |
| `INT8` | `INT8` | `int8` | :white_check_mark: `Int8` | - | :white_check_mark: `tinyint` | `TINYINT` |
| `UINT8` | `UINT8` | `uint8` | :white_check_mark: `UInt8` | - | :white_check_mark: `tinyint unsigned` | |
| `INT16` | `INT16` | `int16` | :white_check_mark: `Int16` | :white_check_mark: `smallint`, `int2`, `smallserial`, `serial2` | :white_check_mark: `smallint` | `SMALLINT`|
| `UINT16` | `UINT16` | `uint16` | :white_check_mark: `UInt16` | - | :white_check_mark: `smallint unsigned` | |
| `INT32` | `INT32` | `int32` | :white_check_mark: `Int32` | :white_check_mark: `integer`, `int`, `int4`, `serial`, `serial4` | :white_check_mark: `mediumint` (upcast до `int32`), `int` |`INT` |
| `UINT32` | `UINT32` | `uint32` | :white_check_mark: `UInt32` | - | :white_check_mark: `mediumint unsigned` (upcast до `uint32`), `int unsigned` | |
| `INT64` | `INT64` | `int64` | :white_check_mark: `Int64` | :white_check_mark: `bigint`, `int8`, `bigserial`, `serial8` | :white_check_mark: `bigint` | `BIGINT`|
| `UINT64` | `UINT64` | `uint64` | :white_check_mark: `UInt64` | - | :white_check_mark: `bigint unsigned` | |
| | | | :x: `Int128` | | | |
| | | | :x: `UInt128` | | | |
| | | | :x: `Int256` | | | |
| | | | :x: `UInt256` | | | |
| `FLOAT` | `FLOAT` | `float32` | :white_check_mark: `Float32` | :white_check_mark: `real`, `float4` | :white_check_mark: `float`, `real` | `REAL`|
| `DOUBLE` | `DOUBLE` | `float64` | :white_check_mark: `Float64` | :white_check_mark: `double precision`, `float8` | :white_check_mark: `double [precision]` | `FLOAT`|
| `DATE` (`uint16`, дни с начала эпохи) | `UINT16` | `time.Time` | :white_check_mark: `Date` (`uint16`, количество дней) | - | - | | 
| `DATE` | `DATE32` (`int32`) | `time.Time` | :white_check_mark:  `Date32` (`int32`, количество дней) | - | :x: `date` (от `1000-01-01' до '9999-12-31`) | | 
| `DATE` (`uint16`, дни с начала эпохи) | `UINT16` | `time.Time` | - | :white_check_mark: `date` (`int32`, дата без времени, диапазон от 4713 г. до н. э. до 5874897 г. н. э.) | | | 
| `DATETIME` (`uint32`, секунды с начала эпохи) | `UINT32` | `time.Time` | :white_check_mark: `DateTime` (:question: уточнить тип, секунды с начала эпохи) | - | :x: `datetime` (от `1000-01-01 00:00:00` дo `9999-12-31 23:59:59`) | | 
| `TIMESTAMP` (`uint64`, микросекунды с начала эпохи) | `UINT64` | `time.Time` | - | :white_check_mark: `timestamp[(p)][without time zone]` (`int64`, микросекунды с начала эпохи) | :x: `timestamp` (от `1970-01-01 00:00:01` UTC до `2038-01-19 03:14:07` UTC) | | 
| `TIMESTAMP` | `UINT64` | `time.Time` | :white_check_mark: `DateTime64` (`int64`, единицы измерения произвольной точности) | - | | | 
| - | - | - | - | :x: `time[(p)][without time zone]` (`int64`, только время суток без даты, разрешение - микросекунды)  | :x: `time` (от `-838:59:59` до `838:59:59`, точность до микросекунд) | | 
| `INTERVAL` (int64, точность до микросекунд) | `INT64` | - | - | - | | | 
| - | - | - | :x: `INTERVAL` (`uint`, 11 разных типов данных в диапазоне от `NANOSECOND` до `YEAR`) | - | | | 
| - | - | - | - | :x: `interval [fields][(p)]` (структура из 3 полей общим размером 16 байт, 13 опций разрешение - микросекунды) | | | 
| - | - | - | - | - | :x: `year` (1 байт, представляет года 1901 от 2155) | | 
| `TZ_DATE` | :x: `STRUCT<UINT16, UINT16>` | `time.Time` | Даты [хранятся](https://clickhouse.com/docs/en/sql-reference/data-types/datetime#usage-remarks) только в формате в unix timestamp, без указания временной зоны, а вот показываются/парсятся уже с учётом временной зоны, которая берётся либо из атрибутов таблицы, либо из настроек сервера и ОС. | - | | | 
| `TZ_DATE` | :x: `STRUCT<UINT32, UINT16>` | `time.Time` | :point_up_2: | - | | | 
| `TZ_TIMESTAMP` | :x: `STRUCT<UINT64, UINT16>` | `time.Time` | :point_up_2: | :x: `timestamp [(p)] with time zone` (`int64`, микросекунды с начала эпохи) | | | 
| `STRING` (строка с произвольными бинарными данными) | `BINARY` | `[]byte` | :white_check_mark: `String`, `FixedString` | :white_check_mark: `bytea` | :white_check_mark: `tinyblob`, `blob`, `mediumblob`, `longblob`, `binary`, `varbinary` | `BINARY`, `VARBINARY`| 
| `UTF8` (текст в UTF-8) | `STRING` | `string` | - | :white_check_mark: `character [(n)]`, `character varying [(n)]`, `text`  | :white_check_mark: `char`, `varchar`, `tinytext`, `text`, `mediumtext`, `longtext` | `CHAR`, `VARCHAR`, `NCHAR`, `NVARCHAR`, `TEXT` | 
| `YSON` | - | - | - | - | | | 
| `JSON` (текстовое представление) | `STRING` | `string` | :x: `JSON` | :white_check_mark: `json` | :x: `JSON` | :x: `json` |
| `UUID` | :x: `BINARY(16)` | - | :x: `UUID` (16 байт) | :x: `uuid` (16 байт) | | |
| `JSON_DOCUMENT` (текстовое представление) | - | - | :question: `JSON` | :x: `json` | | |
| `DYNUMBER` | - | - | - | - | | |
| `Decimal` | :x: `BINARY(16)` | - | :x: `Decimal`, `Decimal32`, `Decimal64`, `Decimal128`, `Decimal256` | :x: `numeric[(p, s)]`, `decimal[(p, s)]` | :x: `decimal[(p,s)]`, `numeric[(p,s)]` | |
| `List` | :x: `LIST` | `[]T` | :question: `ARRAY` | :question: `array` | | |
| `Tuple` | `NULL`, `STRUCT` | - | :question: `TUPLE` | :question: `composite type` | | |
| `Struct` | :x: `STRUCT` | - | :question: `NESTED` | :question: `composite type` | | |
| `Dict` | :x: `STRUCT`, `LIST`, `MAP` | `map[K, V]` | :x: `Map` | :question: | | |
| `Variant` | :x: `DENSE_UNION` | - | - | - | | |
| `Tagged` | - | - | - | - | | |
| - | - | - | :x: `Enum` | :x: `enum` | | |
| - | - | - | :x: `LowCardinality` | - | | |
| - | - | - | :x: `Point`, `Ring`, `Polygon`, `Multipolygon` | :x: `box`, `circle`, `line`, `lseg`, `path`, `point` | | |
| - | - | - | :x: `IPV4`, `IPV6` | :x: `cidr`, `inet`, `macaddr`, `macaddr8` | | |
| - | - | - | :x: `AggregateFunction`, `SimpleAggregateFunction` | :question: псевдотипы | | |
| - | - | - | - | :x: `money` (8 байт, число с плавающей точкой) | | |
| - | - | - | - | :x: `xml` | | |
| - | - | - | - | :x: `int4range`, `int4multirange` | | |
| - | - | - | - | :x: `int8range`, `int8multirange` | | |
| - | - | - | - | :x: `numrange`, `nummultirange` | | |
| - | - | - | - | :x: `tsrange`, `tsmultirange` | | |
| - | - | - | - | :x: `tstzrange`, `tstzmultirange` | | |
| - | - | - | - | :x: `daterange`, `datemultirange` | | |
| | | | | | | |
| | | | | | | |
