# Реализация коннектора Redis

## Анализ возможностей конкурентных систем

### [Trino](https://trino.io/docs/current/connector/redis.html)

1. **Поддерживаемые типы данных**
    - Каждая пара ключ/значение представлена в `Trino` в виде отдельной строки. Строки могут быть разбиты на ячейки с
      помощью `table definition files`.
    - В настоящее время поддерживаются только ключи типа `string` и `zset`, а также значения типа `string` и `hash`
      -типов.

2. **Чтение схемы `Redis`:**
    - Схема конфигурируется вручную в `table definition files` в `json` формате

3. **Поддерживаемые сценарии работы:**
    - Чтение с `table definition files`, с помощью которого данные, например, из словарей, будут "раскладываться" по
      колонкам автоматически
        - Подходит, если формат данных меняется редко
    - Чтение "в лоб"
        - Не требует конфигурационных файлов
        - Данные буду представлены буквально как пара ключ-значение
        - Чтобы "разложить" по колонкам придется написать SQL запрос

**Пример:**

- Поднимем `Redis & Trino`
- Заполним конфигурационный файл `redis.properties`
   ```
   connector.name=redis
   redis.table-names=example_table # optional
   redis.nodes=valkey:6379
   redis.default-schema=default
   redis.table-description-dir=/etc/trino/table-descriptions # optional: путь до table definition files
   redis.hide-internal-columns=false # optional: позволяет видеть в SELECT * приватные колонки
   ```
- Наполним данными - по отдельности запустим команды
   ```
   SET example_table:1 '{"field1": "Alice", "field2": "Wonderland", "field3": "25"}'
   SET example_table:2 '{"field1": "Bob", "field2": "Builder", "field3": "30"}'
   SET example_table:3 '{"field1": "Charlie", "field2": "Chocolate Factory", "field3": "12"}'
   SET test:1 '{"field1": "Charlie", "field2": "Chocolate Factory", "field3": "12"}'
   ```
- Теперь, в `Trino` выполним запрос
   ```
   trino> SELECT _key, _value FROM redis.default.example_table;
         
         _key       |                                _value                                
   -----------------+----------------------------------------------------------------------
    example_table:2 | {"field1": "Bob", "field2": "Builder", "field3": "30"}               
    test:1          | {"field1": "Charlie", "field2": "Chocolate Factory", "field3": "12"} 
    example_table:3 | {"field1": "Charlie", "field2": "Chocolate Factory", "field3": "12"} 
    example_table:1 | {"field1": "Alice", "field2": "Wonderland", "field3": "25"}          
   (4 rows)
   
   Query 20250216_170203_00014_jsiq5, FINISHED, 1 node
   Splits: 1 total, 1 done (100.00%)
   0.12 [4 rows, 249B] [33 rows/s, 2.04KiB/s]
   ```
- Видим, что данные выгрузились в "сыром" виде
- Мы можем "распарсить" их на колонки, обычным SQL запросом, а так же отфильтровать ключи
   ```
   trino> SELECT 
       ->     _key, 
       ->     CAST(json_extract_scalar(_value, '$.field1') AS VARCHAR) AS field1,
       ->     CAST(json_extract_scalar(_value, '$.field2') AS VARCHAR) AS field2,
       ->     CAST(json_extract_scalar(_value, '$.field3') AS INTEGER) AS field3
       -> FROM redis.default.example_table where _key like 'example_table%';
         
         _key       | field1  |      field2       | field3 
   -----------------+---------+-------------------+--------
    example_table:2 | Bob     | Builder           |     30 
    example_table:3 | Charlie | Chocolate Factory |     12 
    example_table:1 | Alice   | Wonderland        |     25 
   (3 rows)
   
   Query 20250216_170739_00022_jsiq5, FINISHED, 1 node
   Splits: 1 total, 1 done (100.00%)
   0.16 [5 rows, 317B] [32 rows/s, 2KiB/s]
   ```

Однако, если все данные однородны и их схема изменяется редко, мы можем создать `table definition file`, в котором
опишем желаемую конвертацию типов. И тогда обычный `SELECT * FROM` будет возвращать результат уже разбитый на колонки,
как выше.

### [Amazon Athena](https://docs.aws.amazon.com/athena/latest/ug/connectors-redis.html)

1. **Чтение схемы `Redis`:**

    - Единственным способом является чтение схемы
      из [AWS Glue](https://docs.aws.amazon.com/athena/latest/ug/connectors-redis.html#connectors-redis-setting-up-databases-and-tables-in-glue)
        - [Ссылка на код](https://github.com/awslabs/aws-athena-query-federation/blob/8480b18200fe0a44a218f08561027effdc8880ff/athena-redis/src/main/java/com/amazonaws/athena/connectors/redis/RedisMetadataHandler.java#L218)
    - Схема во время проброса запроса в нативном формате Redis формируется по самому запросу, поэтому не подходит
        - [Ссылка на код](https://github.com/awslabs/aws-athena-query-federation/blob/8480b18200fe0a44a218f08561027effdc8880ff/athena-redis/src/main/java/com/amazonaws/athena/connectors/redis/RedisMetadataHandler.java#L237C29-L237C56)

2. **Типы данных:**

    - `Redis OSS` connector поддерживает следующие типы данных. Потоки `Redis OSS` не поддерживаются.
        - [String](https://redis.io/glossary/redis-data-structures/)
        - [Hash](https://redis.io/glossary/redis-data-structures/)
        - `Sorted Set` / [ZSet](https://redis.io/glossary/redis-sorted-sets/)

    - Все значения `Redis OSS` извлекаются в виде типа данных `string`. Затем они преобразуются в один из следующих
      типов
      данных `Apache Arrow` в зависимости от того, как определены ваши таблицы в каталоге данных `AWS Glue`.

| AWS Glue data type | Apache Arrow data type |
|--------------------|------------------------|
| `int`              | `INT`                  |
| `string`           | `VARCHAR`              |
| `bigint`           | `BIGINT`               |
| `double`           | `FLOAT8`               |
| `float`            | `FLOAT4`               |
| `smallint`         | `SMALLINT`             |
| `tinyint`          | `TINYINT`              |
| `boolean`          | `BIT`                  |
| `binary`           | `VARBINARY`            |

3. **Поддерживаемые сценарии работы:**

    - либо только чтение - [link](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source.html)
    - либо проброс запросов в нативном формате
      Redis - [link](https://docs.aws.amazon.com/athena/latest/ug/connectors-redis.html#connectors-redis-passthrough-queries)

### [ClickHouse](https://clickhouse.com/docs/en/engines/table-engines/integrations/redis)

1. **Чтение схемы `Redis`:**
    - Либо задается вручную
    - Либо формируется при создании таблицы в `DDL`

2. **Поддерживаемые сценарии работы:**
    - Примитивный пушдаун фильтраций ключа
    - Позволяет пробрасывать запросы в `Redis` из `SELECT` или использовать как `read-only Table Engine`

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
   name1 [type1],
   name2 [type2],
   ...
) ENGINE = Redis({host:port[, db_index[, password[, pool_size]]] |
                  named_collection[, option=value [,..]] }) -- schema
   PRIMARY KEY(primary_key_name);
```