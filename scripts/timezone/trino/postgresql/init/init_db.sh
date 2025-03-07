#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS datetime;
    CREATE TABLE datetime (
        id int,
        ts_without_tz TIMESTAMP WITHOUT TIME ZONE,
        ts_with_tz TIMESTAMP WITH TIME ZONE
    );
    INSERT INTO datetime VALUES (1, '2024-01-01 00:00:00', '2024-01-01 00:00:00 Asia/Tokyo');
EOSQL
