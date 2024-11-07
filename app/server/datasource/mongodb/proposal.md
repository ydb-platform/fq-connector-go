# Реализация коннектора MongoDB

## Анализ возможностей конкурентных систем

### Trino + Presto

1. Преобразование схемы MongoDB

По умолчанию обе системы угадывают тип полей в коллекции по первому документу в ней (при этом в MongoDB не определен порядок документов в коллекции по умолчанию, если не включать сортировку в запросе), и записывают полученную схему в коллекцию `_schema` (название коллекции можно менять через опциональный параметр `mongodb.schema-collection`). Eсли в последующих документах какого-то типа нет, то при чтении в соответсвующей документу строке такое поле ставят равным NULL, новые поля просто пропускаются.

Насколько я понимаю, схема читается из базы данных в каждый релевантный запрос (`show columns` в CLI, `SELECT *` и тд) для того, чтобы ее можно было править вручную в случае некорректного выведения типов и переиспользовать между запросами. Схема записывается один раз в случае, если ее не существует, поэтому ее можно указать вручную и она не будет обновлена.

Trino поддерживает операции записи во внешний источник: `INSERT`, `CREATE TABLE` и `ALTER SCHEMA`. Операция изменения схемы в контексте MongoDB на практике может означать только то, что ее нужно каким-то персистентным образом зафиксировать для того чтобы ей смогли воспользоваться извне, например, при повторном подключении из Trino (самой MongoDB она ни к чему без включения [валидации](https://www.mongodb.com/docs/manual/core/schema-validation/), что здесь не используется).

- Реализация в коде Trino
	- [getTableMetadata](https://github.com/trinodb/trino/blob/ae96ba108799ed3a16340f26da4775bf50bcc641/plugin/trino-mongodb/src/main/java/io/trino/plugin/mongodb/MongoSession.java#L782)
	- [guessTableFields](https://github.com/trinodb/trino/blob/ae96ba108799ed3a16340f26da4775bf50bcc641/plugin/trino-mongodb/src/main/java/io/trino/plugin/mongodb/MongoSession.java#L877)

Полученную схему можно корректировать вручную.
Она должна быть выражена в следующем формате:

```
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

##### MongoDB to Trino type mapping

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


[DBRef](https://www.mongodb.com/docs/manual/reference/database-references/#dbrefs) это структура, идентифицирующая документ:

```
{ 
    "$ref" : collection_name,
    "$id" : document_id,
    "$db" : optional_db_name
}
```

[ROW](https://trino.io/docs/current/language/types.html#row) в Trino - кортеж с полями необязательно одинаковых SQL типов:

`CAST(ROW(1, 2e0) AS ROW(x BIGINT, y DOUBLE))`

##### Trino to MongoDB type mapping

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

- Вычленение даты создания документа, которая зашифрована в поле `_id` типа ObjectId ([документация](https://trino.io/docs/current/connector/mongodb.html#objectid) + [код](https://github.com/trinodb/trino/blob/d28b52f21632ad36bb08e36a753b522383013727/plugin/trino-mongodb/src/main/java/io/trino/plugin/mongodb/ObjectIdFunctions.java#L38)) и вспомогательные функции для него:
    - objectid_timestamp(ObjectId) - вычисляет timestamp с таймзоной
    - timestamp_objectid(timestamp) - строит ObjectId по timestamp с таймзоной

Подобный функционал позволяет фильтровать документы по дате создания, потому что можно скастовать дату в ObjectId:
```
# timestamp -> ObjectId
SELECT timestamp_objectid(TIMESTAMP '2021-08-07 17:51:36 +00:00');
-- ObjectId("610ec8280000000000000000")

# поиск документов, которые были созданы позже указанной даты
db.collection.find({"_id": {"$gt": ObjectId("610ec8280000000000000000")}})
```

- Проброс запроса в нативном формате MongoDB через синтаксис table function - [документация](https://trino.io/docs/current/connector/mongodb.html#table-functions) + [PR](https://github.com/trinodb/trino/pull/14535)


#### Пример работы

##### Данные в MongoDB

mongosh > db.collection.find()
```
[
  {
    _id: ObjectId('6729ae8258c54bdc9c59139e'),
    int32: 42,
    int64: 2147483650,
    str: 'outer',
    array: [ 1, 2, 3 ],
    double: 1.23,
    date: ISODate('2020-05-18T14:10:30.000Z'),
    boolean: true,
    decimal: Decimal128('9823.1297'),
    object: { int32: 13, str: 'inner' }
  },
  {
    _id: ObjectId('6729ae8258c54bdc9c59139f'),
    int32: 67,
    str: 'outer_2',
    str_array: [ 'hi', 'ciao', 'привет' ],
    double: 1,
    date: ISODate('2024-05-18T14:10:30.000Z'),
    boolean: false,
    decimal: Decimal128('103.120'),
    object: { int32: 14, str: 'inner_2' }
  },
  {
    _id: ObjectId('6729ae8258c54bdc9c5913a0'),
    nested: { inner: { field: 27 } }
  }
]
```

##### Выведенная схема в Trino

trino > show columns from mongo.test.types;


| Column  |              Type              |
|---|---|
| int32   | bigint                         |
| int64   | double                         |
| str     | varchar                        |
| array   | array(bigint)                  |
| double  | double                         |
| date    | timestamp(3)                   |
| boolean | boolean                        |
| decimal | decimal(8,4)                   |
| object  | row(int32 bigint, str varchar) |


Нетрудно заметить, что здесь только типы из первого документа =)

##### Данные в Trino

trino > select * from mongo.test.types;

| int32 |    int64     |   str   |   array   | double |          date           | boolean |  decimal  |         object
|---|---|---|---|---|---|---|---|---|
|    42 | 2.14748365E9 | outer   | [1, 2, 3] |   1.23 | 2020-05-18 14:10:30.000 | true    | 9823.1297 | {int32=13, str=inner}
|    67 |         NULL | outer_2 | NULL      |    1.0 | 2024-05-18 14:10:30.000 | false   |  103.1200 | {int32=14, str=inner_2}
|  NULL |         NULL | NULL    | NULL      |   NULL | NULL                    | NULL    |      NULL | NULL


##### Работа с вложенными типами

Я поправила схему в коллекции `_schema`:
```
{
  _id: ObjectId('6729b6fd208d99230add6671'),
  table: 'types',
  fields: [
    { name: '_id', type: 'ObjectId', hidden: true },
    {
      name: 'nested',
      type: 'row("inner" row("field" bigint))',
      hidden: false
    },
  ]
}
```

Это соответствует схеме последнего документа в коллекции:

```
  {
    nested: {
      inner: {
        field: 27,
      }
    }
  }
```

trino> show columns from mongo.test.types;

| Column |             Type             |
|---|---|
| nested | row(inner row(field bigint)) |

trino> select * from mongo.test.types;

|       nested
|---|
| {inner={field=27}}
| NULL
| NULL


### Amazon Athena - [link](https://docs.aws.amazon.com/athena/latest/ug/connectors-docdb.html)

1. Преобразование схемы MongoDB

- Способ по умолчанию: угадывание схемы (в формате типов Apache Arrow) после маленького скана коллекции (10 документов)
	- [inferSchema](https://github.com/awslabs/aws-athena-query-federation/blob/78732deb5c6432c4e526a51a3c663c7f2356a6bf/athena-docdb/src/main/java/com/amazonaws/athena/connectors/docdb/SchemaUtils.java#L72)
	- если одно и то же поле из коллекции в разных документах разного типа, то в Athena оно редуцируется к STRING (в Trino смотрят только на первый документ и если в последующих какого-то типа нет, то его ставят равным NULL, новые поля просто пропускаются)
	- поля считаются nullable
- Доп. способ: чтение схемы из [AWS Glue](https://docs.aws.amazon.com/athena/latest/ug/connectors-docdb.html#connectors-docdb-setting-up-databases-and-tables-in-aws-glue)

2. Отображение систем типов 

- [DocDB - Arrow](https://docs.aws.amazon.com/athena/latest/ug/connectors-docdb.html#connectors-docdb-data-type-support) 
- [Arrow - Athena](https://github.com/awslabs/aws-athena-query-federation/wiki/Supported-Data-Types)

3. Поддерживаемые сценарии работы

- только чтение - [link](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source.html)
- проброс запросов в нативном формате MongoDB - [link](https://docs.aws.amazon.com/athena/latest/ug/connectors-docdb.html#connectors-docdb-passthrough-queries)
- пушдаун предикатов - [makeQuery](https://github.com/awslabs/aws-athena-query-federation/blob/c65b448bd60b58759ced6729eb08a6d4776f9988/athena-docdb/src/main/java/com/amazonaws/athena/connectors/docdb/QueryUtils.java#L111) + [makePredicate](https://github.com/awslabs/aws-athena-query-federation/blob/c65b448bd60b58759ced6729eb08a6d4776f9988/athena-docdb/src/main/java/com/amazonaws/athena/connectors/docdb/QueryUtils.java#L131)

### ClickHouse

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

## Реализация коннектора в YDB

Минимум: 
- Извлечение схемы + type inference с помощью маленького скана коллекции
- `SELECT * FROM ... ` без предикатов в коллекции с гомогенными документами
- Column projection с фильтрацией колонок на уровне коннектора
- Поддержка простых типов: Int32, Long (64-bit integer), Double, String, Object, Array, BSON Date (на стороне YDB они все будут обернуты в Optional)
- Пушдаун фильтров: операторов сравнения, логических операторов, `LIMIT`, `OFFSET`, column projection на уровне MongoDB

Продвинутая реализация
- Пушдаун сложных предикатов, матчинг паттернов с `LIKE`, аггрегатных функций, `ORDER BY`
- Возможность редактирования полученной в коннекторе схемы
- Чтение схемы из специальной коллекции в бд MongoDB, которую создал пользователь, или дополнительного конфигурационного файла
- Поддержка чтения схемы из систем вроде Apache Hive Metastore


#### MongoDB to YDB + Apache Arrow type mapping

|MongoDB|YDB/YQL|Arrow|
|---|---|---|
|Boolean|BOOL|UINT8|
|Int32|INT32|INT32|
|Int64|INT64|INT64|
|Double|DOUBLE|DOUBLE|
|Binary|STRING|BINARY|
|String|UTF8|STRING|
|Object|JSON|?STRING?|
|Array|?JSON/List\<T\>?|?|
|Decimal128 |?Decimal?|DECIMAL128|
|ObjectId (12 bytes)|?Int16/STRING?|?INT16/BINARY?|
|Date (int64, milliseconds since epoch)|?Interval?|DATE64|
