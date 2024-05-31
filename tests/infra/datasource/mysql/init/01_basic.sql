CREATE TABLE IF NOT EXISTS simple (
    id INT NOT NULL,
    tinyint_column TINYINT,
    smallint_column SMALLINT,
    mediumint_column MEDIUMINT NOT NULL,
    unsigned_int_column INT UNSIGNED,
    int_column INT,
    varchar_column VARCHAR(255),
    float_column FLOAT,
    double_column DOUBLE,
    bool_column BOOL
);

INSERT INTO simple VALUES
(1, -1, 2, 45, 234, -234, 'hello', 4.24, null, 1),
(2, -2, null, 21, 532, 234, 'world', -4.24, -12.2, 0),
(3, -2, 3, 42, 532, 234, '!!!', -1.23, 42.1, 1);
