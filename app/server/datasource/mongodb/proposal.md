# Реализация коннектора MongoDB

### Анализ возможностей конкурентных систем

#### Trino + Presto

1. Преобразование схемы MongoDB

По умолчанию обе системы угадывают тип полей в коллекции по первому документу в ней, и записывают полученную схему в коллекцию `_schema` (название коллекции можно менять через опциональный параметр `mongodb.schema-collection`)

- Реализация в коде Trino
	- [getTableMetadata](https://github.com/trinodb/trino/blob/ae96ba108799ed3a16340f26da4775bf50bcc641/plugin/trino-mongodb/src/main/java/io/trino/plugin/mongodb/MongoSession.java#L782)
	- [guessTableFields](https://github.com/trinodb/trino/blob/ae96ba108799ed3a16340f26da4775bf50bcc641/plugin/trino-mongodb/src/main/java/io/trino/plugin/mongodb/MongoSession.java#L877)

Полученную схему можно корректировать вручную.
Она должна быть выражена в следующем формате:

```json
{
    "table": "table_name",
    "fields": [
             {
                "name" : "column_name",
                "type" : "varchar",
                "hidden" : false
             },
            ...
        ]
    }
}
```

[Формат описание схемы](https://trino.io/docs/current/connector/mongodb.html#table-definition)

2. Отображение систем типов
 
 [Типы Trino](https://trino.io/docs/current/language/types.html#language-types--page-root)

MongoDB to Trino type mapping

|MongoDB type|Trino type|Notes|
|---|---|---|
|`Boolean`|`BOOLEAN`||
|`Int32`|`BIGINT`||
|`Int64`|`BIGINT`||
|`Double`|`DOUBLE`||
|`Decimal128`|`DECIMAL(p, s)`||
|`Date`|`TIMESTAMP(3)`||
|`String`|`VARCHAR`||
|`Binary`|`VARBINARY`||
|`ObjectId`|`ObjectId`||
|`Object`|`ROW`||
|`Array`|`ARRAY`|Map to `ROW` if the element type is not unique.|
|`DBRef`|`ROW`||

Trino to MongoDB type mapping

|Trino type|MongoDB type|
|---|---|
|`BOOLEAN`|`Boolean`|
|`BIGINT`|`Int64`|
|`DOUBLE`|`Double`|
|`DECIMAL(p, s)`|`Decimal128`|
|`TIMESTAMP(3)`|`Date`|
|`VARCHAR`|`String`|
|`VARBINARY`|`Binary`|
|`ObjectId`|`ObjectId`|
|`ROW`|`Object`|
|`ARRAY`|`Array`|


3. Поддерживаемые сценарии работы

- SQL запросы
	- [INSERT](https://trino.io/docs/current/sql/insert.html)
	- [DELETE](https://trino.io/docs/current/sql/delete.html)
	- [CREATE TABLE](https://trino.io/docs/current/sql/create-table.html)
	- [CREATE TABLE AS](https://trino.io/docs/current/sql/create-table-as.html)
	- [DROP TABLE](https://trino.io/docs/current/sql/drop-table.html)
	- [ALTER TABLE](https://trino.io/docs/current/sql/alter-table.html)
	- [CREATE SCHEMA](https://trino.io/docs/current/sql/create-schema.html)
	- [DROP SCHEMA](https://trino.io/docs/current/sql/drop-schema.html)
	- [COMMENT](https://trino.io/docs/current/sql/comment.html)

- Вычленение даты создания документа, которая зашифрована в поле `_id` - [link](https://trino.io/docs/current/connector/mongodb.html#objectid)
- Проброс запроса в нативном формате MongoDB через синтаксис table function - [документация](https://trino.io/docs/current/connector/mongodb.html#table-functions) + [PR](https://github.com/trinodb/trino/pull/14535)

#### Amazon Athena - [link](https://docs.aws.amazon.com/athena/latest/ug/connectors-docdb.html)

1. Преобразование схемы MongoDB

- Способ по умолчанию: угадывание схемы (в формате типов Apache Arrow) после маленького скана коллекции (10 документов)
	- [inferSchema](https://github.com/awslabs/aws-athena-query-federation/blob/78732deb5c6432c4e526a51a3c663c7f2356a6bf/athena-docdb/src/main/java/com/amazonaws/athena/connectors/docdb/SchemaUtils.java#L72)
	- поля разных типов преобразуются в STRING
	- поля считаются nullable
- Доп. способ: чтение схемы из [AWS Glue](https://docs.aws.amazon.com/athena/latest/ug/connectors-docdb.html#connectors-docdb-setting-up-databases-and-tables-in-aws-glue)

2. Отображение систем типов 

- [DocDB - Arrow](https://docs.aws.amazon.com/athena/latest/ug/connectors-docdb.html#connectors-docdb-data-type-support) 
- [Arrow - Athena](https://github.com/awslabs/aws-athena-query-federation/wiki/Supported-Data-Types)

3. Поддерживаемые сценарии работы

- только чтение - [link](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source.html)
- проброс запросов в нативном формате MongoDB - [link](https://docs.aws.amazon.com/athena/latest/ug/connectors-docdb.html#connectors-docdb-passthrough-queries)
- пушдаун предикатов - [makeQuery](https://github.com/awslabs/aws-athena-query-federation/blob/c65b448bd60b58759ced6729eb08a6d4776f9988/athena-docdb/src/main/java/com/amazonaws/athena/connectors/docdb/QueryUtils.java#L111) + [makePredicate](https://github.com/awslabs/aws-athena-query-federation/blob/c65b448bd60b58759ced6729eb08a6d4776f9988/athena-docdb/src/main/java/com/amazonaws/athena/connectors/docdb/QueryUtils.java#L131)

#### ClickHouse

Позволяет пробрасывать запросы в MongoDB из SELECT или использовать как read-only Table Engine

В SELECT явно указывается схема в запросе:

```sql
SELECT * FROM mongodb(
'mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
'my_collection',
'log_type String, host String, command String' -- схема
)
```

Для MongoDB engine используется схема создаваемой в бд таблицы, с которой будут сопоставляться документы из MongoDB. Если поле в документе отсутствует, оно заполняется значением по умолчанию или NULL (для nullable колонок).

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (
    name1 [type1],  
) ENGINE = MongoDB(host:port, database, collection, user, password);
```

2. Отображение систем типов - [link](https://clickhouse.com/docs/en/engines/table-engines/integrations/mongodb#types-mappings)

3. Поддерживаемые сценарии работы

- [StorageMongoDB::buildMongoDBQuery](https://github.com/ClickHouse/ClickHouse/blob/2b87bff7340b70a1d212ad28c6faabf2e797612c/src/Storages/StorageMongoDB.cpp#L280)
- Простой пушдаун предикатов - [link](https://clickhouse.com/docs/en/engines/table-engines/integrations/mongodb#supported-clauses)

### Реализация коннектора в YDB

Минимум: 
- `SELECT * FROM ... ` без предикатов в коллекции с гомогенными документами
- Column projection с фильтрацией колонок на уровне коннектора
- Поддержка простых типов: Int32, Long (64-bit integer), Double, String, Object, Array, BSON Date
- Чтение схемы из специальной коллекции в бд MongoDB, которую создал пользователь, или дополнительного конфигурационного файла

Более интересная версия:
- Извлечение схемы + type inference с помощью маленького скана коллекции с возможностью редактирования
- Пушдаун фильтров: операторов сравнения, логических операторов, `LIMIT`, `OFFSET`, column projection на уровне MongoDB

Продвинутая реализация
- Пушдаун сложных предикатов, матчинг паттернов с `LIKE`, аггрегатных функций, `ORDER BY`
- Подд
ержка чтения схемы из систем вроде Apache Hive Metastore

