#!/bin/bash

set -x

/ydb -p tests-ydb-client yql -s '
    CREATE TABLE simple (id Int32 NOT NULL, col1 String NOT NULL, col2 Int32 NOT NULL, PRIMARY KEY (id));
    COMMIT;
    INSERT INTO simple (id, col1, col2) VALUES
      (1, "ydb_a", 10),
      (2, "ydb_b", 20),
      (3, "ydb_c", 30),
      (4, "ydb_d", 40),
      (5, "ydb_e", 50);
    COMMIT;

    CREATE TABLE primitives (
        id Int32 NOT NULL,
        col_01_bool Bool NOT NULL,
        col_02_int8 Int8 NOT NULL,
        col_03_int16 Int16 NOT NULL,
        col_04_int32 Int32 NOT NULL,
        col_05_int64 Int64 NOT NULL,
        col_06_uint8 Uint8 NOT NULL,
        col_07_uint16 Uint16 NOT NULL,
        col_08_uint32 Uint32 NOT NULL,
        col_09_uint64 Uint64 NOT NULL,
        col_10_float Float NOT NULL,
        col_11_double Double NOT NULL,
        col_12_string String NOT NULL,
        col_13_utf8 Utf8 NOT NULL,
        col_14_date Date NOT NULL,
        col_15_datetime Datetime NOT NULL,
        col_16_timestamp Timestamp NOT NULL,
        col_17_json Json NOT NULL,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO
    primitives (id, col_01_bool, col_02_int8, col_03_int16, col_04_int32, col_05_int64, col_06_uint8, col_07_uint16,
                col_08_uint32, col_09_uint64, col_10_float, col_11_double, col_12_string, col_13_utf8,
                col_14_date, col_15_datetime, col_16_timestamp, col_17_json)
    VALUES (1, false, 1, -2, 3, -4, 5, 6, 7, 8, 9.9f, -10.10, "ая", "az",
            Date("1988-11-20"), Datetime("1988-11-20T12:55:28Z"), Timestamp("1988-11-20T12:55:28.123Z"),
            @@{ "friends" : [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}@@
            );
    COMMIT;


    CREATE TABLE optionals (
        id Int32 NOT NULL,
        col_01_bool Optional<Bool>,
        col_02_int8 Optional<Int8>,
        col_03_int16 Optional<Int16>,
        col_04_int32 Optional<Int32>,
        col_05_int64 Optional<Int64>,
        col_06_uint8 Optional<Uint8>,
        col_07_uint16 Optional<Uint16>,
        col_08_uint32 Optional<Uint32>,
        col_09_uint64 Optional<Uint64>,
        col_10_float Optional<Float>,
        col_11_double Optional<Double>,
        col_12_string Optional<String>,
        col_13_utf8 Optional<Utf8>,
        col_14_date Optional<Date>,
        col_15_datetime Optional<Datetime>,
        col_16_timestamp Optional<Timestamp>,
        col_17_json Optional<Json>,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO
    optionals (id, col_01_bool, col_02_int8, col_03_int16, col_04_int32, col_05_int64, col_06_uint8, col_07_uint16,
               col_08_uint32, col_09_uint64, col_10_float, col_11_double, col_12_string, col_13_utf8,
               col_14_date, col_15_datetime, col_16_timestamp, col_17_json)
    VALUES
        (1, true, 1, -2, 3, -4, 5, 6, 7, 8, 9.9f, -10.10, "ая", "az",
            Date("1988-11-20"), Datetime("1988-11-20T12:55:28Z"), Timestamp("1988-11-20T12:55:28.123Z"),
            CAST(@@{ "friends" : [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}@@ AS Json)
        ),
        (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    COMMIT;


    CREATE TABLE datetime (
        id Int32 NOT NULL,
        col_01_date Date NOT NULL,
        col_02_datetime Datetime NOT NULL,
        col_03_timestamp Timestamp NOT NULL,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO
    datetime (id, col_01_date, col_02_datetime, col_03_timestamp)
    VALUES (1, Date("1988-11-20"), Datetime("1988-11-20T12:55:28Z"), Timestamp("1988-11-20T12:55:28.123456Z"));
    COMMIT;

    CREATE TABLE pushdown (
        id Int32 NOT NULL,
        col_01_int Int32,
        col_02_text UTF8,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO pushdown (id, col_01_int, col_02_text) VALUES
        (1, 10, "a"),
        (2, 20, "b"),
        (3, 30, "c"),
        (4, NULL, NULL);
    COMMIT;

    -- YQ-3681
    CREATE TABLE pushdown_strings (
        id Int32 NOT NULL,
        col_01_int Int32,
        col_02_utf8 UTF8,
        col_03_string STRING,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO pushdown_strings (id, col_01_int, col_02_utf8, col_03_string) VALUES
        (1, 10, "a", "a"),
        (2, 20, "b", "b"),
        (3, 30, "c", "c"),
        (4, NULL, NULL, NULL);
    COMMIT;

    -- YQ-4255
    CREATE TABLE pushdown_like (
        id Int32 NOT NULL,
        col_01_string STRING,
        col_02_utf8 UTF8,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO pushdown_like (id, col_01_string, col_02_utf8) VALUES
        (1, "abc", "абв"),
        (2, "def", "где"),
        (3, "ghi", "ёжз"),
        (4, NULL, NULL);
    COMMIT;

    -- YQ-4280
    CREATE TABLE pushdown_regexp (
        id Int32 NOT NULL,
        col_01_string STRING,
        col_02_utf8 UTF8,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO pushdown_regexp (id, col_01_string, col_02_utf8) VALUES
        (1, "123", "123"),
        (2, "абв", "абв"),
        (3, "abc", "abc"),
        (4, NULL, NULL);
    COMMIT;

    CREATE TABLE `parent/child` (
        id INT32 NOT NULL,
        col UTF8 NOT NULL,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO `parent/child` (id, col) VALUES
      (1, "a"),
      (2, "b"),
      (3, "c"),
      (4, "d"),
      (5, "e");
    COMMIT;

    CREATE TABLE `yq-4224` (
        hash UTF8 NOT NULL,
        sbom_s3_path UTF8,
        PRIMARY KEY (hash, sbom_s3_path)
    );
    COMMIT;
    INSERT INTO `yq-4224` (hash, sbom_s3_path) VALUES
      ("6758ddf04f23be19dc7adf08356c697f21dc751aabc1c71b55d340ee920781ca", "a"),
      ("6758ddf04f23be19dc7adf08356c697f21dc751aabc1c71b55d340ee920781cb", NULL);
    COMMIT;

    CREATE TABLE invalid_credentials (
        id Int32 NOT NULL,
        PRIMARY KEY (id)
    );
    COMMIT;
  '

# YQ-3494
/ydb -p tests-ydb-client yql -s "
    CREATE TABLE json_document (
        id INT32 NOT NULL,
        data JsonDocument NOT NULL,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO json_document (id, data) VALUES
      (1, JsonDocument('{\"key1\": \"value1\"}')),
      (2, JsonDocument('{\"key2\": \"value2\"}'));
    COMMIT;
"

