#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    DROP TABLE IF EXISTS connector.simple;
    CREATE TABLE connector.simple (id Int32, col1 String, col2 Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO connector.simple (*) VALUES 
        (1, 'ch_a', 10) \
        (2, 'ch_b', 20) \
        (3, 'ch_c', 30) \
        (4, 'ch_d', 40) \
        (5, 'ch_e', 50);
EOSQL

clickhouse client -n <<-EOSQL
    DROP TABLE IF EXISTS connector.primitives;
    CREATE TABLE connector.primitives (
        id Int32,
        col_01_boolean Boolean,
        col_02_int8 Int8,
        col_03_uint8 UInt8,
        col_04_int16 Int16,
        col_05_uint16 UInt16,
        col_06_int32 Int32,
        col_07_uint32 UInt32,
        col_08_int64 Int64,
        col_09_uint64 UInt64,
        col_10_float32 Float32,
        col_11_float64 Float64,
        col_12_string String,
        col_13_string FixedString(13),
        col_14_date Date,
        col_15_date32 Date32,
        col_16_datetime DateTime,
        col_17_datetime64 DateTime64(3)
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO connector.primitives (*) VALUES 
        (1, False, 2, 3, 4, 5, 6, 7, 8, 9, 10.10, 11.11, 'az', 'az', '1988-11-20', '1988-11-20', '1988-11-20 12:55:08', '1988-11-20 12:55:08.123') \
        (2, True, -2, 3, -4, 5, -6, 7, -8, 9, -10.10, -11.11, 'буки', 'буки', '2023-03-21', '2023-03-21', '2023-03-21 11:21:31', '2023-03-21 11:21:31.456');
EOSQL

clickhouse client -n <<-EOSQL
    DROP TABLE IF EXISTS connector.optionals;
    CREATE TABLE connector.optionals (
        id Int32,
        col_01_boolean Nullable(Boolean),
        col_02_int8 Nullable(Int8),
        col_03_uint8 Nullable(UInt8),
        col_04_int16 Nullable(Int16),
        col_05_uint16 Nullable(UInt16),
        col_06_int32 Nullable(Int32),
        col_07_uint32 Nullable(UInt32),
        col_08_int64 Nullable(Int64),
        col_09_uint64 Nullable(UInt64),
        col_10_float32 Nullable(Float32),
        col_11_float64 Nullable(Float64),
        col_12_string Nullable(String),
        col_13_string Nullable(FixedString(13)),
        col_14_date Nullable(Date),
        col_15_date32 Nullable(Date32),
        col_16_datetime Nullable(DateTime),
        col_17_datetime64 Nullable(DateTime64(3))
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO connector.optionals (*) VALUES 
        (1, False, 2, 3, 4, 5, 6, 7, 8, 9, 10.10, 11.11, 'az', 'az', '1988-11-20', '1988-11-20', '1988-11-20 12:55:08', '1988-11-20 12:55:08.123') \
        (2, True, -2, 3, -4, 5, -6, 7, -8, 9, -10.10, -11.11, 'буки', 'буки', '2023-03-21', '2023-03-21', '2023-03-21 11:21:31', '2023-03-21 11:21:31.456') \
        (3, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
EOSQL
