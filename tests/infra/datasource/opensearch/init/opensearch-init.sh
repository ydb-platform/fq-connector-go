#!/bin/bash
set -e

echo "==============================="
echo "Initialization script started!"
echo "==============================="

sleep 10
until curl -s http://localhost:9200; do
  echo "Waiting for OpenSearch to start..."
  sleep 5
done

echo "==============================="
echo "Simple mappings!"
echo "==============================="

curl -X PUT "http://localhost:9200/simple" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "bool_field": { "type": "boolean" },
      "int32_field": { "type": "integer" },
      "int64_field": { "type": "long" },
      "float_field": { "type": "float" },
      "double_field": { "type": "double" },
      "string_field": { "type": "keyword" },
      "timestamp_field": { "type": "date" }
    }
  }
}'

curl -X POST "http://localhost:9200/simple/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "bool_field": true, "int32_field": 42, "int64_field": 1234567890123, "float_field": 1.5, "double_field": 2.71828, "string_field": "text_value1", "timestamp_field": "2023-01-01T00:00:00Z" }
{ "index": { "_id": "1" } }
{ "bool_field": false, "int32_field": -100, "int64_field": -987654321, "float_field": -3.14, "double_field": 0.0, "string_field": "text_value2", "timestamp_field": "2023-02-15T12:00:00Z" }
{ "index": { "_id": "2" } }
{ "bool_field": true, "int32_field": 0, "int64_field": 0, "float_field": 0.0, "double_field": -1.2345, "string_field": "text_value3", "timestamp_field": "2023-03-20T18:30:00Z" }
'

echo "==============================="
echo "List mappings!"
echo "==============================="

curl -X PUT "http://localhost:9200/list" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "name": { "type": "keyword" },
      "tags": { "type": "keyword" }
    },
    "_meta": {
        "tags": "list"
      }
  }
}'

curl -X POST "http://localhost:9200/list/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "name": "Alice", "tags": ["developer", "engineer"] }
{ "index": { "_id": "1" } }
{ "name": "Bob", "tags": ["designer"] }
'

echo "==============================="
echo "Struct mappings!"
echo "==============================="

curl -X PUT "http://localhost:9200/nested" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "name": { "type": "keyword" },
      "nested": {
        "type": "object",
        "properties": {
          "bool_field": { "type": "boolean" },
          "int32_field": { "type": "integer" },
          "int64_field": { "type": "long" },
          "float_field": { "type": "float" },
          "double_field": { "type": "double" },
          "string_field": { "type": "keyword" },
          "timestamp_field": { "type": "date" },
          "binary_field": { "type": "binary" }
        }
      }
    }
  }
}'


curl -X POST "http://localhost:9200/nested/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "name": "Alice", "nested": { "bool_field": true, "int32_field": 42, "int64_field": 1234567890123, "float_field": 3.14, "double_field": 3.1415926535912345678910101, "string_field": "value1", "timestamp_field": "2023-07-20T12:00:00Z", "binary_field": "SGVsbG8gQWxpY2U=" } }
{ "index": { "_id": "1" } }
{ "name": "Bob", "nested": { "bool_field": false, "int32_field": 24, "int64_field": 9876543210987, "float_field": 2.71, "double_field": 2.7182818284512345678910101, "string_field": "value2", "timestamp_field": "2023-07-21T15:30:00Z", "binary_field": "SGVsbG8gQm9i" } }
'

echo "==============================="
echo "List of Struct mappings!"
echo "==============================="

curl -X PUT "http://localhost:9200/nested_list" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "_meta": {
        "employees": "list",
        "employees.skills": "list"
    },
    "properties": {
      "company": { "type": "keyword" },
      "employees": {
        "type": "nested",
        "properties": {
          "id": { "type": "integer" },
          "name": { "type": "keyword" },
          "skills": {
            "type": "nested",
            "properties": {
              "name": { "type": "keyword" },
              "level": { "type": "integer" }
            }
          }
        }
      }
    }
  }
}'

curl -X POST "http://localhost:9200/nested_list/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "1" } }
{
  "company": "Tech Corp",
  "employees": [
    {
      "id": 1,
      "name": "Alice",
      "skills": [
        { "name": "Go", "level": 5 },
        { "name": "Python", "level": 4 }
      ]
    },
    {
      "id": 2,
      "name": "Bob",
      "skills": [
        { "name": "Java", "level": 3 },
        { "name": "JavaScript", "level": 5 }
      ]
    }
  ]
}
{ "index": { "_id": "2" } }
{
  "company": "Dev Inc",
  "employees": [
    {
      "id": 3,
      "name": "Charlie",
      "skills": [
        { "name": "Rust", "level": 4 },
        { "name": "C++", "level": 5 }
      ]
    }
  ]
}
'

echo "==============================="
echo "Optional fields demo"
echo "==============================="

curl -X PUT "http://localhost:9200/optional" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "a": { "type": "keyword" },
      "b": { "type": "integer" }
    }
  }
}'

curl -X POST "http://localhost:9200/optional/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "1" } }
{ "a": "value1", "b": 10 }
{ "index": { "_id": "2" } }
{ "a": "value2", "b": 20, "c": "new_field" }
{ "index": { "_id": "3" } }
{ "a": "value3", "b": 30, "d": 3.14 }
{ "index": { "_id": "4" } }
{ "a": "value4", "c": "another_value", "d": 2.71 }
'

echo "==============================="
echo "Ids demo"
echo "==============================="

curl -X PUT "http://localhost:9200/ids" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "type": { "type": "keyword" },
    }
  }
}'

curl -X POST "http://localhost:9200/ids/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": 1 } }
{ "type": "int" }
{ "index": { "_id": 0.1 } }
{ "type": "double" }
{ "index": { "_id": "string" } }
{ "type": "string" }
'

echo "==============================="
echo "Successfully initialized!!!"
echo "==============================="