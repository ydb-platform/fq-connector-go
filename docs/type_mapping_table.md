# Таблица соответствия типов

| YDB | Arrow | Go | ClickHouse | PostgreSQL | MySQL | MS SQL Server |
| --- | ----- | -- | ---------- | ---------- | ----- | ------ |
| `BOOL` | `UINT8` | `bool` | :white_check_mark: `Bool` | :white_check_mark: `boolean`, `bool` (1 байт) | | |
| - | - | - | - | :x: `bit[(n)]`, `bit varying[(n)]`, `varbit` (битовые строки, состоящие из 0 и 1)| | |
| `INT8` | `INT8` | `int8` | :white_check_mark: `Int8` | - | | |
| `UINT8` | `UINT8` | `uint8` | :white_check_mark: `UInt8` | - | | |
| `INT16` | `INT16` | `int16` | :white_check_mark: `Int16` | :white_check_mark: `smallint`, `int2`, `smallserial`, `serial2` | | |
| `UINT16` | `UINT16` | `uint16` | :white_check_mark: `UInt16` | - | | |
| `INT32` | `INT32` | `int32` | :white_check_mark: `Int32` | :white_check_mark: `integer`, `int`, `int4`, `serial`, `serial4` | | |
| `UINT32` | `UINT32` | `uint32` | :white_check_mark: `UInt32` | - | | |
| `INT64` | `INT64` | `int64` | :white_check_mark: `Int64` | :white_check_mark: `bigint`, `int8`, `bigserial`, `serial8` | | |
| `UINT64` | `UINT64` | `uint64` | :white_check_mark: `UInt64` | - | | |
| | | | :x: `Int128` | | | |
| | | | :x: `UInt128` | | | |
| | | | :x: `Int256` | | | |
| | | | :x: `UInt256` | | | |
| `FLOAT` | `FLOAT` | `float32` | :white_check_mark: `Float32` | :white_check_mark: `real`, `float4` | | |
| `DOUBLE` | `DOUBLE` | `float64` | :white_check_mark: `Float64` | :white_check_mark: `double precision`, `float8` | | |
