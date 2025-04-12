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