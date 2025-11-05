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

| BigQuery       | Trino                       |
| -------------- | --------------------------- |
| BOOLEAN        | BOOLEAN                     |
| INT64          | BIGINT                      |
| FLOAT64        | DOUBLE                      |
| NUMERIC        | DECIMAL(P,S)                |
| BIGNUMERIC     | DECIMAL(P,S)                |
| DATE           | DATE                        |
| DATETIME       | TIMESTAMP(6)                |
| STRING         | VARCHAR                     |
| BYTES          | VARBINARY                   |
| TIME           | TIME(6)                     |
| TIMESTAMP      | TIMESTAMP(6) WITH TIME ZONE |
| GEOGRAPHY      | VARCHAR                     |
| JSON           | JSON                        |
| ARRAY          | ARRAY                       |
| RECORD(STRUCT) | ROW                         |


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

#### Использование

Для Trino подготовлен пример использования источника BigQuery: 
[github.com/vladDotH/bigquery-examples/tree/main/trino](https://github.com/vladDotH/bigquery-examples/tree/main/trino)

### Amazon [Athena](https://docs.aws.amazon.com/athena/latest/ug/connectors-bigquery.html)

Athena использует только Arrow схему для чтения типов [BigQueryRowReader.java#L54](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-google-bigquery/src/main/java/com/amazonaws/athena/connectors/google/bigquery/BigQueryRowReader.java#L54).
В остальном процесс обработки Arrow батчей аналогичный.

Athena поддерживает pushdown следующих предикатов: 
1. Логические: AND, OR, NOT
2. Сравнение: EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, IS_DISTINCT_FROM, NULL_IF, IS_NULL
3. Арифметические: ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULUS, NEGATE
4. Прочие: LIKE_PATTERN, IN

Также Athena позволяет пропустить [(passthrough)](https://docs.aws.amazon.com/athena/latest/ug/federated-query-passthrough.html) 
запрос напрямую в BigQuery с помощью `system.query`, например:
```
SELECT * FROM TABLE(
        system.query(
            query => 'SELECT * FROM somedataset.sometable LIMIT 10' // Google SQL query
        ))
```

### Apache [Doris](https://doris.apache.org/docs/3.0/lakehouse/datalake-analytics/bigquery)

Apache Doris решает проблему проще и напрямую использует [коннектор из Trino](https://github.com/apache/doris-thirdparty/tree/trino-435)

Маппинг типов для Doris:
| BigQuery   | Doris        |
| ---------- | ------------ |
| BOOLEAN    | BOOLEAN      |
| INT64      | BIGINT       |
| FLOAT64    | DOUBLE       |
| NUMERIC    | DECIMAL(p,s) |
| BIGNUMERIC | DECIMAL(p,s) |
| STRING     | STRING       |
| BYTES      | STRING       |
| DATE       | DATE         |
| DATETIME   | DATETIME     |
| TIME       | STRING       |
| TIMESTAMP  | DATETIME     |
| GEOGRAPHY  | STRING       |
| ARRAY      | ARRAY        |
| RECORD     | STRUCT       |


### Apache [Spark](https://spark.apache.org/docs/latest/)

Spark поддерживает BigQuery в качестве источника для [Spark SQL Datasource API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources).
Spark также использует Storage API; и поддерживает pushdown предикатов.

Маппинг типов для Spark:
| BigQuery                 | Spark                              |
| ------------------------ | ---------------------------------- |
| BOOLEAN                  | BooleanType                        |
| INT64                    | LongType                           |
| FLOAT64                  | DoubleType                         |
| NUMERIC                  | DecimalType                        |
| BIGNUMERIC               | DecimalType                        |
| STRING                   | StringType                         |
| BYTES                    | BinaryType                         |
| DATE                     | DateType                           |
| DATETIME                 | StringType, TimestampNTZType       |
| TIME                     | LongType, StringType*              |
| TIMESTAMP                | TimestampType                      |
| ARRAY                    | ArrayType                          |
| STRUCT                   | StructType                         |
| JSON                     | StringType                         |
| ARRAY<STRUCT<key,value>> | MapType (BigQuery has no Map type) |

Для Spark подготовлен пример использования с источником BigQuery: 
[github.com/vladDotH/bigquery-examples/tree/main/spark](https://github.com/vladDotH/bigquery-examples/tree/main/spark)

## Реализация коннектора в YDB

Возможные варианты для реалзиации:
- Использовать REST API и SQL-запросы
- Использовать gRPC Storage API

В обоих случаях можно получить схему (Google SQL или Arrow) для вывода типов.
Storage API позволяет читать данные в нескольких потоках (по разным колонкам), но не поддерживает сложные операции по типу группировок,
а только простую фильтрацию (where условия).

Первоначальные шаги реализации:
1. Получение схемы
2. Получение данных (если из Storage API то в одном стриме)
3. Поддержка примитивных типов
4. Пушдаун предикатов в фильтры

Дополнительные возможные действия:
1. Поддержка составных типов
2. Распараллелить стримы Storage API для получения данных


Маппинг типов:

| BigQuery    | YDB/YQL                              | Arrow                   |
| ----------- | ------------------------------------ | ----------------------- |
| Array<T>    | List<T>                              | List<T>                 |
| Boolean     | Bool                                 | Bool                    |
| Bytes       | String                               | Binary                  |
| Date        | Date                                 | Date                    |
| Datetime    | Datetime                             | Timestamp               |
| GEOGRAPHY   | String                               | Utf8                    |
| INTERVAL    | Interval                             | INTERVAL_MONTH_DAY_NANO |
| JSON        | JSON                                 | Utf8                    |
| INT64       | Int64                                | Int64                   |
| NUMERIC     | Decimal                              | Decimal                 |
| BIGNUMERIC  | Decimal                              | Decimal256              |
| FLOAT64     | Double                               | Double                  |
| RANGE<T>    | Tuple<T,T> or Struct<start:T, end:T> | Struct<start T, end T>  |
| STRING      | Utf8                                 | Utf8                    |
| STRUCT<k V> | Structure<k:V>                       | Struct                  |
| TIME        | Timestamp                            | TIME64                  |
| TIMESTAMP   | Timestamp                            | Timestamp               |

