#!/bin/bash

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
  