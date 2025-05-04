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
echo "Optional fields"
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

echo "========================================="
echo "Setting up OpenSearch test data for pushdowns"
echo "========================================="

# 1. Pushdown Projection
echo "Creating pushdown_projection index..."
curl -X PUT "http://localhost:9200/pushdown_projection" -H 'Content-Type: application/json' -d'
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

curl -X POST "http://localhost:9200/pushdown_projection/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "bool_field": true, "int32_field": 42, "int64_field": 1234567890123, "float_field": 1.5, "double_field": 2.71828, "string_field": "text_value1", "timestamp_field": "2023-01-01T00:00:00Z" }
{ "index": { "_id": "1" } }
{ "bool_field": false, "int32_field": -100, "int64_field": -987654321, "float_field": -3.14, "double_field": 0.0, "string_field": "text_value2", "timestamp_field": "2023-02-15T12:00:00Z" }
{ "index": { "_id": "2" } }
{ "bool_field": true, "int32_field": 0, "int64_field": 0, "float_field": 0.0, "double_field": -1.2345, "string_field": "text_value3", "timestamp_field": "2023-03-20T18:30:00Z" }
'

# 2. Pushdown IsNull/IsNotNull
echo "Creating pushdown_null_checks index..."
curl -X PUT "http://localhost:9200/pushdown_null_checks" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "int32": { "type": "integer" },
      "double": { "type": "double" },
      "boolean": { "type": "boolean" },
      "int64": { "type": "long" },
      "string": { "type": "keyword" },
      "objectid": { "type": "keyword" }
    }
  }
}'

curl -X POST "http://localhost:9200/pushdown_null_checks/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "int32": 42, "double": 1.1, "boolean": true, "int64": 123, "string": "exists", "objectid": "507f1f77bcf86cd799439011" }
{ "index": { "_id": "1" } }
{ "int32": null, "double": null, "boolean": null, "int64": 456, "string": null, "objectid": null }
{ "index": { "_id": "2" } }
{ "int32": 24, "double": 2.2, "boolean": false, "int64": null, "string": "exists2", "objectid": "507f1f77bcf86cd799439012" }
'

# 3. Pushdown Comparisons
echo "Creating pushdown_comparisons index..."
curl -X PUT "http://localhost:9200/pushdown_comparisons" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "ind": { "type": "integer" },
      "int32": { "type": "integer" },
      "int64": { "type": "long" },
      "string": { "type": "keyword" },
      "binary": { "type": "binary" },
      "double": { "type": "double" },
      "boolean": { "type": "boolean" },
      "objectid": { "type": "keyword" }
    }
  }
}'

curl -X POST "http://localhost:9200/pushdown_comparisons/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "ind": 0, "int32": 64, "int64": 23423, "string": "outer", "binary": "q80=", "double": 1.1, "boolean": false, "objectid": "507f1f77bcf86cd799439011" }
{ "index": { "_id": "1" } }
{ "ind": 1, "int32": 32, "int64": 12345, "string": "inner", "binary": "q81=", "double": 2.2, "boolean": true, "objectid": "507f1f77bcf86cd799439012" }
'

# 4. Pushdown String Comparisons
echo "Creating pushdown_string_comps index..."
curl -X PUT "http://localhost:9200/pushdown_string_comps" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "a": { "type": "text", "fields": { "keyword": { "type": "keyword" } } }
    }
  }
}'

curl -X POST "http://localhost:9200/pushdown_string_comps/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "a": "abc def" }
{ "index": { "_id": "1" } }
{ "a": "def abc" }
{ "index": { "_id": "2" } }
{ "a": "toast is great" }
'

# 5. Pushdown LG Comparison
echo "Creating pushdown_two_columns index..."
curl -X PUT "http://localhost:9200/pushdown_two_columns" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "ind": { "type": "integer" },
      "a": { "type": "integer" }
    }
  }
}'

curl -X POST "http://localhost:9200/pushdown_two_columns/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "ind": 5, "a": 6 }
{ "index": { "_id": "1" } }
{ "ind": 3, "a": 2 }
{ "index": { "_id": "2" } }
{ "ind": 1, "a": 1 }
'

# 6. Pushdown Conjunction/Disjunction
echo "Creating pushdown_logical_ops index..."
curl -X PUT "http://localhost:9200/pushdown_logical_ops" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "a": { "type": "integer" },
      "b": { "type": "keyword" },
      "ind": { "type": "integer" }
    }
  }
}'

curl -X POST "http://localhost:9200/pushdown_logical_ops/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "a": 1, "b": "hello", "ind": 0 }
{ "index": { "_id": "1" } }
{ "a": 2, "b": "hi", "ind": 1 }
{ "index": { "_id": "2" } }
{ "a": 1, "b": "world", "ind": 2 }
'

# 7. Pushdown Between
echo "Creating pushdown_between index..."
curl -X PUT "http://localhost:9200/pushdown_between" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "ind": { "type": "integer" }
    }
  }
}'

curl -X POST "http://localhost:9200/pushdown_between/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "ind": 2 }
{ "index": { "_id": "1" } }
{ "ind": 3 }
{ "index": { "_id": "2" } }
{ "ind": 4 }
{ "index": { "_id": "3" } }
{ "ind": 5 }
'

# 8. Pushdown In
echo "Creating pushdown_in index..."
curl -X PUT "http://localhost:9200/pushdown_in" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "ind": { "type": "integer" },
      "b": { "type": "keyword" }
    }
  }
}'

curl -X POST "http://localhost:9200/pushdown_in/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "ind": 1, "b": "one" }
{ "index": { "_id": "1" } }
{ "ind": 4, "b": "four" }
{ "index": { "_id": "2" } }
{ "ind": 6, "b": "six" }
{ "index": { "_id": "3" } }
{ "ind": 2, "b": "two" }
{ "index": { "_id": "4" } }
{ "ind": 5, "b": "hi" }
'

# 9. Pushdown Regex
echo "Creating pushdown_regex index..."
curl -X PUT "http://localhost:9200/pushdown_regex" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "a": { "type": "text", "fields": { "keyword": { "type": "keyword" } } }
    }
  }
}'

curl -X POST "http://localhost:9200/pushdown_regex/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "a": "toast is great" }
{ "index": { "_id": "1" } }
{ "a": "roast beef" }
{ "index": { "_id": "2" } }
{ "a": "coastal area" }
'

# Refresh all indices
echo "Refreshing all indices..."
curl -X POST "http://localhost:9200/_refresh"


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