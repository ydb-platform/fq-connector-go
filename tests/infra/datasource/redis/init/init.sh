#!/bin/bash
set -e

# Для кейса stringOnly: добавляем ключи с простыми строковыми значениями.
redis-cli FLUSHALL
redis-cli SET stringKey1 "value1"
redis-cli SET stringKey2 "value2"

# Для кейса hashOnly: добавляем hash-ключи.
redis-cli FLUSHALL
redis-cli HSET hashKey1 field1 "hashValue1" field2 "hashValue2"
redis-cli HSET hashKey2 field1 "hashValue3" field2 "hashValue4" field3 "hashValue5"

# Для кейса mixed: один ключ со строковым значением и один с hash-значением.
redis-cli FLUSHALL
redis-cli SET mixedKey1 "mixedString"
redis-cli HSET mixedKey2 hashField1 "mixedHash1" hashField2 "mixedHash2"

# Для пустой базы (empty) не добавляем никаких ключей.
redis-cli FLUSHALL
