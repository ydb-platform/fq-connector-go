#!/bin/bash
set -e

# For the "stringOnly" case: add keys with simple string values.
redis-cli FLUSHALL
redis-cli SET stringOnly:stringKey1 "value1"
redis-cli SET stringOnly:stringKey2 "value2"

# For the "hashOnly" case: add hash keys.
redis-cli HSET hashOnly:hashKey1 field1 "hashValue1" field2 "hashValue2"
redis-cli HSET hashOnly:hashKey2 field1 "hashValue3" field2 "hashValue4" field3 "hashValue5"

# For the "mixed" case: one key with a string value and one with a hash value.
redis-cli SET mixed:stringKey1 "mixedString"
redis-cli HSET mixed:hashKey2 hashField1 "mixedHash1" hashField2 "mixedHash2"
