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
      "id": { "type": "integer" },
      "a": { "type": "keyword" },
      "b": { "type": "integer" },
      "c": { "type": "long" }
    }
  }
}'

curl -X POST "http://localhost:9200/simple/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "id": 0, "a": "jelly", "b": 2000, "c": 13 }
{ "index": { "_id": "1" } }
{ "id": 1, "a": "butter", "b": -20021, "c": 0 }
{ "index": { "_id": "2" } }
{ "id": 2, "a": "toast", "b": 2076, "c": 2076 }
'

echo "==============================="
echo "List mappings!"
echo "==============================="

curl -X PUT "http://localhost:9200/list" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "id": { "type": "integer" },
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
{ "id": 0, "name": "Alice", "tags": ["developer", "engineer"] }
{ "index": { "_id": "1" } }
{ "id": 1, "name": "Bob", "tags": ["designer"] }
'

echo "==============================="
echo "Struct mappings!"
echo "==============================="

curl -X PUT "http://localhost:9200/nested" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "id": { "type": "integer" },
      "name": { "type": "keyword" },
      "address": {
        "type": "object",
        "properties": {
          "city": { "type": "keyword" },
          "country": { "type": "keyword" }
        }
      }
    }
  }
}'

curl -X POST "http://localhost:9200/nested/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "0" } }
{ "id": 0, "name": "Alice", "address": { "city": "New York", "country": "USA" } }
{ "index": { "_id": "1" } }
{ "id": 1, "name": "Bob", "address": { "city": "San Francisco", "country": "USA" } }
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
      "id": { "type": "integer" },
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
  "id": 0,
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
  "id": 1,
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
      "id": { "type": "integer" },
      "a": { "type": "keyword" },
      "b": { "type": "integer" }
    }
  }
}'

curl -X POST "http://localhost:9200/optional/_bulk" -H 'Content-Type: application/json' -d'
{ "index": { "_id": "1" } }
{ "id": 1, "a": "value1", "b": 10 }
{ "index": { "_id": "2" } }
{ "id": 2, "a": "value2", "b": 20, "c": "new_field" }
{ "index": { "_id": "3" } }
{ "id": 3, "a": "value3", "b": 30, "d": 3.14 }
{ "index": { "_id": "4" } }
{ "id": 4, "a": "value4", "c": "another_value", "d": 2.71 }
'

echo "==============================="
echo "Successfully initialized!!!"
echo "==============================="