#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username gpadmin --dbname template1 <<-EOSQL
    DROP TABLE IF EXISTS simple;
    CREATE TABLE simple (id int, col1 text, col2 int);
    INSERT INTO simple VALUES (1, 'pg_a', 10);
    INSERT INTO simple VALUES (2, 'pg_b', 20);
    INSERT INTO simple VALUES (3, 'pg_c', 30);
    INSERT INTO simple VALUES (4, 'pg_d', 40);
    INSERT INTO simple VALUES (5, 'pg_e', 50);
EOSQL

psql -v ON_ERROR_STOP=1 --username gpadmin --dbname template1 <<-EOSQL
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
        col_24_date date
    );
    INSERT INTO primitives VALUES (
        1, false, 2, 3, DEFAULT, DEFAULT, 6, 7, 8, DEFAULT, DEFAULT, 11, 12, DEFAULT, DEFAULT,
        15.15, 16.16, 17.17, 18.18, 'az', 'az', 'az', 'az',
        '1988-11-20 12:55:28.123000', '1988-11-20');
    INSERT INTO primitives VALUES (
        2, true, -2, -3, DEFAULT, DEFAULT, -6, -7, -8, DEFAULT, DEFAULT, -11, -12, DEFAULT, DEFAULT,
        -15.15, -16.16, -17.17, -18.18, 'буки', 'буки', 'буки', 'буки',
        '2023-03-21 11:21:31.456000', '2023-03-21');
    INSERT INTO primitives VALUES (
        3, NULL, NULL, NULL, DEFAULT, DEFAULT, NULL,
        NULL, NULL, DEFAULT, DEFAULT, NULL, NULL,
        DEFAULT, DEFAULT, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL
        );
EOSQL

psql -v ON_ERROR_STOP=1 --username gpadmin --dbname template1 <<-EOSQL
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

psql -v ON_ERROR_STOP=1 --username gpadmin --dbname template1 <<-EOSQL
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
