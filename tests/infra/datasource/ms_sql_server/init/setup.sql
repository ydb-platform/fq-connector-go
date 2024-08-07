DROP TABLE IF EXISTS simple;

CREATE TABLE simple (
    id INT IDENTITY(1,1) PRIMARY KEY, 
    col1 NVARCHAR(7) NOT NULL,
    col2 INTEGER NOT NULL
);

INSERT INTO simple (id, col1, col2) VALUES 
    (1, 'mssql_a', 10),
    (2, 'mssql_b', 20),
    (3, 'mssql_c', 30);

SELECT * FROM simple;

-- CREATE TABLE simple (id INT IDENTITY(1,1) PRIMARY KEY, col1 NVARCHAR(7) NOT NULL, col2 INTEGER NOT NULL);
-- INSERT INTO simple (col1, col2) VALUES ('mssql_a', 10), ('mssql_b', 20), ('mssql_c', 30);
-- SELECT * FROM simple;

-- ### Numeric Data Types
-- 1. bit – Integer that can be 0, 1, or NULL.
-- 2. tinyint – Integer from 0 to 255.
-- 3. smallint – Integer from -32,768 to 32,767.
-- 4. int – Integer from -2,147,483,648 to 2,147,483,647.
-- 5. bigint – Integer from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.
-- 6. decimal(p, s) – Fixed precision and scale numbers.
-- 7. numeric(p, s) – Equivalent to decimal.
-- 8. smallmoney – Monetary data from -214,748.3648 to 214,748.3647.
-- 9. money – Monetary data from -922,337,203,685,477.5808 to 922,337,203,685,477.5807.
-- 10. float(n) – Floating point number.
-- 11. real – Floating point number.

-- ### Date and Time Data Types
-- 1. date – Stores date data.
-- 2. time – Stores time of day data.
-- 3. datetime – Stores date and time data.
-- 4. datetime2 – Extended date and time data.
-- 5. smalldatetime – Stores date and time data.
-- 6. datetimeoffset – Date and time with time zone awareness.

-- ### Character Strings
-- 1. char(n) – Fixed length non-Unicode data.
-- 2. varchar(n) – Variable length non-Unicode data.
-- 3. text – Variable length non-Unicode large data.

-- ### Unicode Character Strings
-- 1. nchar(n) – Fixed length Unicode data.
-- 2. nvarchar(n) – Variable length Unicode data.
-- 3. ntext – Variable length Unicode large data.

-- ### Binary Data Types
-- 1. binary(n) – Fixed length binary data.
-- 2. varbinary(n) – Variable length binary data.
-- 3. image – Variable length binary large data.

-- CREATE TABLE users (
--     id INT IDENTITY(1,1) NOT NULL,
--     username NVARCHAR(50) NOT NULL,
--     password NVARCHAR(50) NOT NULL,
--     email NVARCHAR(255),
--     PRIMARY KEY(id)
-- );

-- INSERT INTO users (username, password, email) VALUES 
--     ('user1', 'password1', 'user1@email.com'),
--     ('user2', 'password2', 'user2@email.com'),
--     ('user3', 'password3', 'user3@email.com'),
--     ('user4', 'password4', 'user4@email.com'),
--     ('user5', 'password5', 'user5@email.com'),
--     ('user6', 'password6', 'user6@email.com'),
--     ('user7', 'password7', 'user7@email.com'),
--     ('user8', 'password8', 'user8@email.com'),
--     ('user9', 'password9', 'user9@email.com'),
--     ('user10', 'password10', 'user10@email.com');
