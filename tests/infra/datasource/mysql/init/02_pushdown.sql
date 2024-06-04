CREATE TABLE pushdown (
    id INT NOT NULL,
    int_column INT,
    varchar_column VARCHAR(255)
);

INSERT INTO pushdown (id, int_column, varchar_column) VALUES
(1, 10, 'a'),
(2, 20, 'b'),
(3, 30, 'c'),
(4, NULL, NULL);
