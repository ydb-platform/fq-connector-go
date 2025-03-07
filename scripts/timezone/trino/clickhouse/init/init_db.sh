#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    DROP TABLE IF EXISTS db.datetime;
    CREATE TABLE db.datetime (
        id Int32,
        datetime DateTime(3),
        datetime_explicit_tz DateTime(3, 'Asia/Tokyo')
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.datetime (*) VALUES 
        (1, '2024-01-01 00:00:00', '2024-01-01 00:00:00');
EOSQL
