# Реализация коннектора к OpenSearch

## Ликбез про OpenSearch

[OpenSearch](https://opensearch.org/docs/latest/getting-started/intro/) – система поисковых и аналитических
инструментов, которая работает с неоднородными данным – документами, в составе индекса.

Сам движок не имеет жесткой схемы, о которой мы привыкли говорить в семействе реляционных СУБД, однако для более
удобного хранения документов и индексации по полям
есть ["mapping"](https://opensearch.org/docs/latest/field-types/#dynamic-mapping),
который умеет динамически выводить [типы данных](https://opensearch.org/docs/latest/field-types/#dynamic-mapping-types)
для документов в индексе. Отношения также можно
задать [явно](https://opensearch.org/docs/latest/field-types/#explicit-mapping), что поможет избежать конфликтов и
разночтений при запросах.

#### Request

```curl -X GET "http://localhost:9200/my_index/_mapping```

#### Response

```json
{
  "my_index": {
    "mappings": {
      "properties": {
        "age": {
          "type": "integer"
        },
        "email": {
          "type": "keyword"
        },
        "name": {
          "type": "text"
        }
      },
      "_meta": {
        "application": "YDB OpenSearch connector",
        "version": "0.0.1",
        "author": "Arslan Gin"
      }
    }
  }
}      
```

Также в индексе есть возможность хранить метаданные в
поле ["_meta"](https://opensearch.org/docs/latest/field-types/metadata-fields/meta/) для использованиях в клиентских
приложениях.

## Анализ возможностей систем-конкурентов

### [Trino](https://trino.io/docs/current/connector/opensearch.html)

### 1. Преобразование схемы OpenSearch

#### 1.1 Запрос метаданных

Каждый раз, когда выполняется запрос к внешнему источнику данных, Trino запрашивает актуальную схему данных.  
Для OpenSearch это происходит через вызов API `_mapping` индекса. Этот запрос выполняется в методе [
`getIndexMetadata`](https://github.com/trinodb/trino/blob/8d4ba2a80b9ec807b08dac699a58e8d09b63d707/plugin/trino-opensearch/src/main/java/io/trino/plugin/opensearch/client/OpenSearchClient.java#L444).

#### 1.2 Извлечение метаданных

Из ответа `_mapping` Trino извлекает:

- Информацию из поля `_meta` (проставляется явно в поле "trino") для работы
  с [массивами](https://trino.io/docs/current/connector/opensearch.html#array-types)
  и [сложными структурами даных](https://trino.io/docs/current/connector/opensearch.html#raw-json-transform)
- Описание полей из раздела `properties`.

Далее эти данные обрабатываются в методе [
`parseType`](https://github.com/trinodb/trino/blob/8d4ba2a80b9ec807b08dac699a58e8d09b63d707/plugin/trino-opensearch/src/main/java/io/trino/plugin/opensearch/client/OpenSearchClient.java#L485).
На этом этапе Trino анализирует типы полей OpenSearch, такие как:

- `date`
- `scaled_float`
- `nested`
- `object`

Все остальные типы временно классифицируются как примитивы.

#### 1.3 Конвертация типов OpenSearch в типы Trino

После анализа типов OpenSearch происходит их преобразование в соответствующие типы данных Trino. Это реализовано в
методе [
`toTrino`](https://github.com/trinodb/trino/blob/8d4ba2a80b9ec807b08dac699a58e8d09b63d707/plugin/trino-opensearch/src/main/java/io/trino/plugin/opensearch/OpenSearchMetadata.java#L275).

### 2. Отображение систем типов

В Trino поддерживаются следующие [типы даных](https://trino.io/docs/current/language/types.html#language-types--page-root).

[Отображение](https://trino.io/docs/current/connector/opensearch.html#opensearch-type-to-trino-type-mapping) из OpenSearch в Trino.

| OpenSearch type | Trino type       | Notes                                                                 |
|-----------------|------------------|----------------------------------------------------------------------|
| BOOLEAN         | BOOLEAN          |                                                                      |
| DOUBLE          | DOUBLE           |                                                                      |
| FLOAT           | REAL             |                                                                      |
| BYTE            | TINYINT          |                                                                      |
| SHORT           | SMALLINT         |                                                                      |
| INTEGER         | INTEGER          |                                                                      |
| LONG            | BIGINT           |                                                                      |
| KEYWORD         | VARCHAR          | Используется для точного поиска строк.                               |
| TEXT            | VARCHAR          | Для полнотекстового поиска. Может быть дополнен подполем `keyword`.  |
| DATE            | TIMESTAMP        | Для получения дополнительной информации см. раздел "Date types".      |
| IPADDRESS       | IP               | Представляет IP-адреса (например, IPv4 или IPv6).                    |
| NESTED          | ARRAY(ROW(...))  | Вложенные объекты преобразуются в массивы строк или объектов.        |
| OBJECT          | ROW(...)         | Объекты преобразуются в строки с точечной нотацией.                  |

[ROW](https://trino.io/docs/current/language/types.html#row) в Trino - кортеж с полями необязательно одинаковых SQL типов.


### 3. Поддерживаемые сценарии
Trino предоставляет [globaly available](https://trino.io/docs/current/language/sql-support.html#sql-globally-available) и [read](https://trino.io/docs/current/language/sql-support.html#sql-read-operations) запросы для доступа к данным и метаданным в каталоге OpenSearch.

### 4. Производительность

#### 4.1 Параллельное чтение
Trino запрашивает данные с нескольких узлов кластера OpenSearch для параллельной обработки запросов.

#### 4.2 Пушдаун предикатов
В интеграции с OpenSearch поддерживаются только следующие типы данных для [пушдауна](https://trino.io/docs/current/optimizer/pushdown.html#predicate-pushdown).

| OpenSearch type | Trino type       | Пример предиката в Trino                       | Примечания                                                                  |
|-----------------|------------------|------------------------------------------------|-----------------------------------------------------------------------------|
| boolean         | BOOLEAN          | `WHERE is_active = true`                        | Логические значения поддерживаются напрямую.                                |
| double          | DOUBLE           | `WHERE score > 90.5`                            | Числовые предикаты поддерживаются для всех числовых типов.                  |
| float           | REAL             | `WHERE temperature < 36.6`                      |                                                                             |
| byte            | TINYINT          | `WHERE status_code = 1`                         |                                                                             |
| short           | SMALLINT         | `WHERE priority > 5`                            |                                                                             |
| integer         | INTEGER          | `WHERE age BETWEEN 20 AND 30`                   |                                                                             |
| long            | BIGINT           | `WHERE user_id = 123456789`                     |                                                                             |
| keyword         | VARCHAR          | `WHERE city = 'New York'`                       | Точные совпадения для строковых полей типа `keyword`.                       |
| date            | TIMESTAMP        | `WHERE registration_date >= '2023-01-01'`       | Даты поддерживаются, если они хранятся в формате ISO 8601.                  |
| ip              | IP               | `WHERE ip_address = '192.168.1.1'`              | Поддерживаются точные совпадения для IP-адресов.                            |

### Примеры

Создадим индекс с массивами и вложенными объектами, посмотрим, как Trino справится.

```json
   curl -X PUT "http://localhost:9200/complex_index_1?pretty" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "user_id": {
        "type": "keyword"
      },
      "full_name": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "age": {
        "type": "integer"
      },
      "is_active": {
        "type": "boolean"
      },
      "registration_date": {
        "type": "date"
      },
      "address": {
        "type": "object",
        "properties": {
          "city": {
            "type": "keyword"
          },
          "street": {
            "type": "text"
          },
          "coordinates": {
            "type": "object",
            "properties": {
              "latitude": {
                "type": "float"
              },
              "longitude": {
                "type": "float"
              }
            }
          }
        }
      },
      "hobbies": {
        "type": "keyword"
      },
      "projects": {
        "type": "nested",
        "properties": {
          "project_id": {
            "type": "keyword"
          },
          "project_name": {
            "type": "text"
          },
          "tasks": {
            "type": "nested",
            "properties": {
              "task_id": {
                "type": "keyword"
              },
              "task_name": {
                "type": "text"
              },
              "completed": {
                "type": "boolean"
              }
            }
          }
        }
      }
    }
  },
  "_meta": {
    "trino": {
      "projects": {
        "isArray": true,
        "tasks": {
          "isArray": true
        }
      },
      "hobbies": {
        "isArray": true
      }
    }
  }
}'
```

Далее добавим докуммент в соответствие с маппингом.

```json
curl -X PUT "http://localhost:9200/complex_index_1/_doc/1?pretty" -H 'Content-Type: application/json' -d'
{
  "user_id": "user_001",
  "full_name": "Alice Johnson",
  "age": 30,
  "is_active": true,
  "registration_date": "2022-01-15",
  "address": {
    "city": "New York",
    "street": "5th Avenue",
    "coordinates": {
      "latitude": 40.7128,
      "longitude": -74.006
    }
  },
  "hobbies": ["reading", "cycling"],
  "projects": [
    {
      "project_id": "proj_001",
      "project_name": "Data Analysis",
      "tasks": [
        { "task_id": "task_001", "task_name": "Data Cleaning", "completed": true },
        { "task_id": "task_002", "task_name": "Data Visualization", "completed": false }
      ]
    },
    {
      "project_id": "proj_002",
      "project_name": "Machine Learning",
      "tasks": [
        { "task_id": "task_003", "task_name": "Model Training", "completed": true }
      ]
    }
  ]
}'
```

А теперь посмотрим на вывод Trino.
```shell
trino> SELECT * FROM elasticsearch.default.complex_index_1;
                                        address                                        | age |   full_name   |      hobbies       | is_active |                                                                                                                                                        projects                                                                                                                                                         |    registration_date    | user_id  
---------------------------------------------------------------------------------------+-----+---------------+--------------------+-----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------+----------
 {city=New York, coordinates={latitude=40.7128, longitude=-74.006}, street=5th Avenue} |  30 | Alice Johnson | [reading, cycling] | true      | [{project_id=proj_001, project_name=Data Analysis, tasks=[{completed=true, task_id=task_001, task_name=Data Cleaning}, {completed=false, task_id=task_002, task_name=Data Visualization}]}, {project_id=proj_002, project_name=Machine Learning, tasks=[{completed=true, task_id=task_003, task_name=Model Training}]}] | 2022-01-15 00:00:00.000 | user_001 
(1 row)

Query 20250302_161024_00017_2ydqr, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.11 [1 rows, 609B] [8 rows/s, 5.22KiB/s]

```

Все типы данных разложились в плоскую структуру.

```shell
trino> DESCRIBE elasticsearch.default.complex_index_1;
      Column       |                                                             Type                                                              | Extra | Comment 
-------------------+-------------------------------------------------------------------------------------------------------------------------------+-------+---------
 address           | row(city varchar, coordinates row(latitude real, longitude real), street varchar)                                             |       |         
 age               | integer                                                                                                                       |       |         
 full_name         | varchar                                                                                                                       |       |         
 hobbies           | array(varchar)                                                                                                                |       |         
 is_active         | boolean                                                                                                                       |       |         
 projects          | array(row(project_id varchar, project_name varchar, tasks array(row(completed boolean, task_id varchar, task_name varchar)))) |       |         
 registration_date | timestamp(3)                                                                                                                  |       |         
 user_id           | varchar                                                                                                                       |       |         
(8 rows)

Query 20250302_161641_00018_2ydqr, FINISHED, 1 node
Splits: 11 total, 11 done (100.00%)
0.55 [8 rows, 745B] [14 rows/s, 1.33KiB/s]

```

### AWS Athena

### 1. Преобразование схемы OpenSearch
[parseMapping](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-elasticsearch/src/main/java/com/amazonaws/athena/connectors/elasticsearch/ElasticsearchSchemaUtils.java#L54)
Метод, который парсит ответ `GET <index>/_mapping` – метаданные из OpenSearch `_meta`.
В самом `_meta` необходимо описать поля индекса, которые должны быть интерпретированы как список или массив.
Если не выполнить этот шаг, запросы будут возвращать только первый элемент в поле списка.
Когда вы указываете свойство _meta, имена полей должны полностью соответствовать вложенным структурам JSON
(например, address.street, где улица - это вложенное поле внутри структуры адреса).
```json
PUT movies/_mapping 
{ 
  "_meta": { 
    "actor": "list", 
    "genre": "list" 
  } 
}
```

### 2. Отображение систем типов
[OpenSearch -> Apache Arrow](https://docs.aws.amazon.com/athena/latest/ug/connectors-opensearch.html)

[Apache Arrow -> Aws Athena](https://github.com/awslabs/aws-athena-query-federation/wiki/Supported-Data-Types)

### 3. Поддерживаемые сценарии

Поддерживается только сценарий чтения.
Можно также использовать [следующие DDL операции](https://docs.aws.amazon.com/athena/latest/ug/connectors-opensearch.html#connectors-opensearch-running-sql-queries).


```sql
SHOW DATABASES in `lambda:function_name`

SHOW TABLES in `lambda:function_name`.domain

DESCRIBE `lambda:function_name`.domain.index
```

### 4. Производительность

#### 4.1 Параллельное чтение

Athena OpenSearch connector поддерживает параллельное сканирование на основе сегментов.
Коннектор использует информацию о работоспособности кластера, полученную из экземпляра OpenSearch, для создания нескольких запросов на поиск документов.
Запросы разделены для каждого сегмента и выполняются одновременно.

#### 4.2 Пушдаун предиктов

Также поддерживается пушдаун предикатов

**Query**:
```sql
SELECT * FROM "lambda:elasticsearch".movies.movies 
WHERE year >= 1955 AND year <= 1962 OR year = 1996
```

**Predicate**:
```sql
(_exists_:year) AND year:([1955 TO 1962] OR 1996)
```

### Реализация коннектора к OpenSearch в YDB

### Минимум:

#### 1. Извлечение схемы из метаданных `/_mapping`

#### 2. SELECT * FROM ...  без предикатов в коллекции с гомогенными документами

#### 3. Column projection с фильтрацией колонок на уровне коннектора

#### 4. Пушдаун фильтров: операторов сравнения, логических операторов, LIMIT, OFFSET


### OpenSearch - YDB - Apache Arrow mapping

| OpenSearch          | YDB/YQL           | Arrow        |
|---------------------|-------------------|--------------|
| `boolean`           | `BOOL`            | `UINT8`      |
| `integer`           | `Int32`           | `INT32`      |
| `long`              | `Int64`           | `INT64`      |
| `double`            | `Double`          | `DOUBLE`     |
| `binary`            | `String`          | `BINARY`     |
| `keyword` / `text`  | `Utf8`            | `VARCHAR`    |
| `object`            | `Json`            | `STRUCT`     |
| `nested`            | `List<T>`         | `LIST`       |
| `scaled_float`      | `Decimal`         | `DECIMAL128` |
| `date`              | `Interval`        | `DATE64`     |



