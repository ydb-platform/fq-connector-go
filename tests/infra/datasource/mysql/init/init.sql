-- CREATE TABLE IF NOT EXISTS simple (
--     id INT NOT NULL,
--     tinyint_column TINYINT,
--     smallint_column SMALLINT,
--     mediumint_column MEDIUMINT NOT NULL,
--     unsigned_int_column INT UNSIGNED,
--     int_column INT,
--     varchar_column VARCHAR(255),
--     float_column FLOAT,
--     double_column DOUBLE,
--     bool_column BOOL
-- );

-- INSERT INTO simple VALUES
-- (1, -1, 2, 45, 234, -234, 'hello', 4.24, null, 1),
-- (2, -2, null, 21, 532, 234, 'world', -4.24, -12.2, 0),
-- (3, -2, 3, 42, 532, 234, '!!!', -1.23, 42.1, 1);

DROP TABLE IF EXISTS simple;
CREATE TABLE simple (
    id INT NOT NULL, 
    col1 VARCHAR(7),
    col2 INTEGER
);
INSERT INTO simple VALUES (1, 'mysql_a', 10),
                          (2, 'mysql_b', 20),
                          (3, 'mysql_c', 30);


CREATE TABLE pushdown (
    id INT NOT NULL,
    int_column INT,
    varchar_column VARCHAR(255)
);

INSERT INTO pushdown VALUES
                     (1, 10, 'a'),
                     (2, 20, 'b'),
                     (3, 30, 'c'),
                     (4, NULL, NULL);
