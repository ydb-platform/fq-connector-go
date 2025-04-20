# Реализация коннектора BigQuery

## BigQuery API

BigQuery поддерживает как минимум 2 API для работы с данными.
1. SQL или HTTP запросы через [HTTP REST API](https://cloud.google.com/bigquery/docs/reference/bigqueryconnection/rest)
2. [Storage API](https://cloud.google.com/bigquery/docs/reference/storage) через gRPC

Storage API позволяет получать данные в форматах Avro и Arrow, и 
разбивать получение данных на параллельные потоки. 
Также поддерживает выбор строк и фильтрацию. 

## Анализ возможностей конкурентных систем

### [Trino](https://trino.io/docs/current/connector/bigquery.html)

Так как Trino использует Storage API, типы колонок таблицы можно получить
из Arrow/Avro схемы, которая приходит при запросе.
В Trino это происходит здесь: [BigQuerySplitSource.java#L255](https://github.com/trinodb/trino/blob/master/plugin/trino-bigquery/src/main/java/io/trino/plugin/bigquery/BigQuerySplitSource.java#L255)

#### Преобразование типов

BigQuery во многом схожа с другими SQL СУБД, поэтому её типы без проблем переводятся в типы Trino 
[следующим образом](https://trino.io/docs/current/connector/bigquery.html#type-mapping):

| BigQuery   | Trino                       |
| ---------- | --------------------------- |
| BOOLEAN    | BOOLEAN                     |
| INT64      | BIGINT                      |
| FLOAT64    | DOUBLE                      |
| NUMERIC    | DECIMAL(P,S)                |
| BIGNUMERIC | DECIMAL(P,S)                |
| DATE       | DATE                        |
| DATETIME   | TIMESTAMP(6)                |
| STRING     | VARCHAR                     |
| BYTES      | VARBINARY                   |
| TIME       | TIME(6)                     |
| TIMESTAMP  | TIMESTAMP(6) WITH TIME ZONE |
| GEOGRAPHY  | VARCHAR                     |
| JSON       | JSON                        |
| ARRAY      | ARRAY                       |
| RECORD     | ROW                         |


И наобороот:

| Trino        | BigQuery |
| ------------ | -------- |
| BOOLEAN      | BOOLEAN  |
| VARBINARY    | BYTES    |
| DATE         | DATE     |
| DOUBLE       | FLOAT    |
| BIGINT       | INT64    |
| DECIMAL(P,S) | NUMERIC  |
| VARCHAR      | VARCHAR  |
| TIMESTAMP(6) | DATETIME |

#### Поддерживаемые SQL операции 

Trino поддерживает [следующие операции](https://trino.io/docs/current/connector/bigquery.html#sql-support) для BigQuery:
1. [INSERT](https://trino.io/docs/current/sql/insert.html)
2. [DELETE](https://trino.io/docs/current/sql/delete.html)
3. [CREATE TABLE](https://trino.io/docs/current/sql/create-table.html)
4. [CREATE TABLE AS](https://trino.io/docs/current/sql/create-table-as.html)
5. [DROP TABLE](https://trino.io/docs/current/sql/drop-table.html)
6. [ALTER TABLE](https://trino.io/docs/current/sql/alter-table.html)
7. [CREATE SCHEMA](https://trino.io/docs/current/sql/create-schema.html)
8. [DROP SCHEMA](https://trino.io/docs/current/sql/drop-schema.html)
9. [COMMENT](https://trino.io/docs/current/sql/comment.html)

