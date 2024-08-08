DROP TABLE IF EXISTS simple;

CREATE TABLE simple (
    id INTEGER PRIMARY KEY, 
    col1 NTEXT NOT NULL,
    col2 INTEGER NOT NULL
);

INSERT INTO simple (id, col1, col2) VALUES 
    (1, 'ms_sql_server_a', 10),
    (2, 'ms_sql_server_b', 20),
    (3, 'ms_sql_server_c', 30);

SELECT * FROM simple;

CREATE TABLE primitives (
    id INTEGER PRIMARY KEY,
    col_01_bit BIT,
    col_02_tinyint TINYINT,
    col_03_smallint SMALLINT,
    col_04_int INT,
    col_05_bigint BIGINT,
    col_06_float FLOAT,
    col_07_real REAL,
    col_08_char CHAR(8),
    col_09_varchar VARCHAR(8),
    col_10_text TEXT,
    col_11_nchar NCHAR(8),
    col_12_nvarchar NVARCHAR(8),
    col_13_ntext NTEXT,
    col_14_binary BINARY(8),
    col_15_varbinary VARBINARY(8),
    col_16_image IMAGE 
);

INSERT INTO primitives VALUES
    (0, 1, 2, 3, 4, 5, 6.6, 7.7, 'az', 'az', 'az', 'az', 'az', 'az', 0x1234567890ABCDEF, 0x1234567890ABCDEF, 0x1234567890ABCDEF),
    (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (2, 0, 2, -3, -4, -5, -6.6, -7.7, N'буки', N'буки', N'буки', N'буки', N'буки', N'буки', 0x1234567890ABCDEF, 0x1234567890ABCDEF, 0x1234567890ABCDEF);

SELECT * FROM primitives;

-- ### Date and Time Data Types
-- 1. date – Stores date data.
-- 2. time – Stores time of day data.
-- 3. datetime – Stores date and time data.
-- 4. datetime2 – Extended date and time data.
-- 5. smalldatetime – Stores date and time data.
-- 6. datetimeoffset – Date and time with time zone awareness.
