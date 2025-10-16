#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS simple;
    CREATE TABLE simple (id int, col1 text, col2 int);
    INSERT INTO simple VALUES (1, 'pg_a', 10);
    INSERT INTO simple VALUES (2, 'pg_b', 20);
    INSERT INTO simple VALUES (3, 'pg_c', 30);
    INSERT INTO simple VALUES (4, 'pg_d', 40);
    INSERT INTO simple VALUES (5, 'pg_e', 50);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS primitives;
    CREATE TABLE primitives (
        id int,
        col_01_bool bool,
        col_02_smallint smallint,
        col_03_int2 int2,
        col_04_smallserial smallserial,
        col_05_serial2 serial2,
        col_06_integer integer,
        col_07_int int,
        col_08_int4 int4,
        col_09_serial serial,
        col_10_serial4 serial4,
        col_11_bigint bigint,
        col_12_int8 int8,
        col_13_bigserial bigserial,
        col_14_serial8 serial8,
        col_15_real real,
        col_16_float4 float4,
        col_17_double_precision double precision,
        col_18_float8 float8,
        col_19_bytea bytea,
        col_20_character_n character(20),
        col_21_character_varying_n character varying(21),
        col_22_text text,
        col_23_timestamp timestamp,
        col_24_date date,
        col_25_json json,
        col_26_uuid UUID,
        col_27_numeric_int numeric(10, 0),
        col_28_numeric_rational numeric(4, 2)
    );
    INSERT INTO primitives VALUES (
        1, false, 2, 3, DEFAULT, DEFAULT, 6, 7, 8, DEFAULT, DEFAULT, 11, 12, DEFAULT, DEFAULT,
        15.15, 16.16, 17.17, 18.18, 'az', 'az', 'az', 'az',
        '1988-11-20 12:55:28.123000', '1988-11-20', 
        '{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}'::json,
        'dce06500-b56b-412b-bc39-f9fafb602663',
        1, 11.11
        );
    INSERT INTO primitives VALUES (
        2, true, -2, -3, DEFAULT, DEFAULT, -6, -7, -8, DEFAULT, DEFAULT, -11, -12, DEFAULT, DEFAULT,
        -15.15, -16.16, -17.17, -18.18, 'буки', 'буки', 'буки', 'буки',
        '2023-03-21 11:21:31.456000', '2023-03-21',
        '{ "TODO" : "unicode" }'::json,
        'b18cafa2-9892-4515-843d-e8ee9bd9a858',
        -2, -22.22
        );
    INSERT INTO primitives VALUES (
        3, NULL, NULL, NULL, DEFAULT, DEFAULT, NULL,
        NULL, NULL, DEFAULT, DEFAULT, NULL, NULL,
        DEFAULT, DEFAULT, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL,
        NULL, NULL
        );
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS datetime;
    CREATE TABLE datetime (
        id int,
        col_01_timestamp timestamp,
        col_02_date date
    );
    INSERT INTO datetime VALUES (1, '1950-05-27 01:02:03.111111', '1950-05-27');
    INSERT INTO datetime VALUES (2, '1988-11-20 12:55:28.123000', '1988-11-20');
    INSERT INTO datetime VALUES (3, '2023-03-21 11:21:31.456000', '2023-03-21');
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS pushdown;
    CREATE TABLE pushdown (
        id int,
        col_01_int int,
        col_02_text text
    );
    INSERT INTO pushdown (id, col_01_int, col_02_text) VALUES \
        (1, 10, 'a'), \
        (2, 20, 'b'), \
        (3, 30, 'c'), \
        (4, NULL, NULL);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS pushdown_decimal;
    CREATE TABLE pushdown_decimal (
        id int,
        col_27_numeric_int numeric(10, 0),
        col_28_numeric_rational numeric(4, 2)
    );
    INSERT INTO pushdown_decimal (id, col_27_numeric_int, col_28_numeric_rational) VALUES \
        (1, 1, 11.11), \
        (2, -2, -22.22);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS primary_key_int;
    CREATE TABLE primary_key_int (
        id int PRIMARY KEY,
        text_col text
    );
    INSERT INTO primary_key_int VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c'),
        (4, 'd'),
        (5, 'e');
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS primary_key_bigint;
    CREATE TABLE primary_key_bigint (
        id bigint PRIMARY KEY,
        text_col text
    );
    INSERT INTO primary_key_bigint VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c'),
        (4, 'd'),
        (5, 'e');
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS primary_key_numeric_10_0;
    CREATE TABLE primary_key_numeric_10_0 (
        id numeric(10, 0) PRIMARY KEY,
        text_col text
    );
    INSERT INTO primary_key_numeric_10_0 VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c'),
        (4, 'd'),
        (5, 'e');
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS primary_key_numeric_4_2;
    CREATE TABLE primary_key_numeric_4_2 (
        id numeric(4, 2) PRIMARY KEY,
        text_col text
    );
    INSERT INTO primary_key_numeric_4_2 VALUES
        (1.00, 'a'),
        (2.50, 'b'),
        (3.75, 'c'),
        (4.25, 'd'),
        (5.99, 'e');
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS primary_key_numeric;
    CREATE TABLE primary_key_numeric (
        id numeric PRIMARY KEY,
        text_col text
    );
    INSERT INTO primary_key_numeric VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c'),
        (4, 'd'),
        (5, 'e');
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    ANALYZE;
EOSQL