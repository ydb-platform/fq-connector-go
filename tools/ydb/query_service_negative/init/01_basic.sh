#!/bin/bash

/ydb -p tests-ydb-client yql -s '

    CREATE TABLE simple (id Int32 NOT NULL, col1 String, col2 Int32, PRIMARY KEY (id));
    COMMIT;
    INSERT INTO simple (id, col1, col2) VALUES
      (1, "ydb_a", 10),
      (2, "ydb_b", 20),
      (3, "ydb_c", 30),
      (4, "ydb_d", 40),
      (5, "ydb_e", 50),
      (6, "ydb_g", NULL);
    COMMIT;
  '
  