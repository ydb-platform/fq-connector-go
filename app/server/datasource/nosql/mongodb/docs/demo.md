# Пример выполнения запроса в MongoDB из локальной инсталляции YDB

## Готовим данные в источнике

Запускаем монгу в корневой директории кода коннектора fq-connector-go

```sh
sudo docker compose -f ./tests/infra/datasource/docker-compose.yaml up -d mongodb
```

Заходим в mongosh

```sh
mongosh --username admin ----authenticationDatabase connector
```

Внутри mongosh:

```
# указываем базу данных
use connector;

# Добавляем три новых записи в коллекцию `primitives`, выставляя поле _id сами.
# Я делаю так в тестах коннектора, потому что это упрощает вспомогательный код интеграционных тестов.

db.primitives.insertMany([
    {
        _id: Int32(0),
        int32: Int32(42),
        int64: Long(23423),
        string: "hello",
        double: 1.22,
        boolean: true,
        binary: Binary.createFromHexString("aaaa"),
        objectid: ObjectId('171e75500ecde1c75c59139e'),
    },
    {
        _id: Int32(1),
        int32: Int32(13),
        int64: Long(13),
        string: "hi",
        double: 1.23,
        boolean: false,
        binary: Binary.createFromHexString("abab"),
        objectid: ObjectId('271e75500ecde1c75c59139e'),
    },
    {
        _id: Int32(2),
        int32: Int32(15),
        int64: Long(15),
        string: "bye",
        double: 1.24,
        boolean: false,
        binary: Binary.createFromHexString("acac"),
        objectid: ObjectId('371e75500ecde1c75c59139e'),
    },
]);

>>> {
>>>   acknowledged: true,
>>>   insertedIds: { '0': Int32(0), '1': Int32(1), '2': Int32(2) }
>>> }


# Добавляем три новых записи в коллекцию `bar`.
# Здесь поле _id заполнит MongoDB, самостоятельно сгенерив ObjectId (https://www.mongodb.com/docs/manual/reference/bson-types/#objectid) для каждого документа:

db.bar.insertMany( [
   {
      a: 'jelly',
      b: Int32(2000),
      c: Long(13),
   },
   {
      a: 'butter',
      b: Int32(-20021),
      c: Long(0),
   },
   {
      a: 'toast',
      b: Int32(2076),
      c: Long(2076),
   }
]);

>>> {
>>>   acknowledged: true,
>>>   insertedIds: {
>>>     '0': ObjectId('67f3c139171bfd11df51e944'),
>>>     '1': ObjectId('67f3c139171bfd11df51e945'),
>>>     '2': ObjectId('67f3c139171bfd11df51e946')
>>>   }
>>> }
```

Запускаем коннектор
```
make run 
```

## Готовим файлы для выполнения запросов

schema.yql
```sql
CREATE OBJECT mongodb_local_password (TYPE SECRET) WITH (value = "password");

CREATE EXTERNAL DATA SOURCE mongodb_external_datasource WITH (
    SOURCE_TYPE="MongoDB",
    LOCATION="localhost:27017",
    AUTH_METHOD="BASIC",
    LOGIN="admin",
    DATABASE_NAME="connector",
    PASSWORD_SECRET_NAME="mongodb_local_password",
    READING_MODE="TABLE",
    UNSUPPORTED_TYPE_DISPLAY_MODE="UNSUPPORTED_OMIT",
    UNEXPECTED_TYPE_DISPLAY_MODE="UNEXPECTED_AS_NULL"
);
```

app_conf.txt
```
FeatureFlags {
    EnableExternalDataSources: true
    EnableScriptExecutionOperations: true
}

QueryServiceConfig {
    AvailableExternalDataSources: "MongoDB"
    Generic {
        Connector {
            Endpoint {
                host: "localhost"
                port: 2130
            }
            UseSsl: false
        }

        DefaultSettings {
            Name: "DateTimeFormat"
            Value: "YQL"
        }
    }
}
```

## Выполняем запросы в YDB с помощью клиента kqprun

### SELECT *

#### Запрос c табличным представлением

В качестве настройки представления типа ObjectId здесь используется значение по умолчанию - YQL String.

Файл с телом запроса

data.yql
```sql
SELECT * FROM mongodb_external_datasource.primitives;
```

Команда запроса

```sh
./ydb/tests/tools/kqprun/kqprun -s ydb/schema.yql -p ydb/data.yql --app-config=ydb/app_conf.txt --result-format="full-proto"
```

Вывод результата выполнения команды без логов:

```
columns {
  name: "_id"
  type {
    type_id: INT32
  }
}
columns {
  name: "binary"
  type {
    optional_type {
      item {
        type_id: STRING
      }
    }
  }
}
columns {
  name: "boolean"
  type {
    optional_type {
      item {
        type_id: BOOL
      }
    }
  }
}
columns {
  name: "double"
  type {
    optional_type {
      item {
        type_id: DOUBLE
      }
    }
  }
}
columns {
  name: "int32"
  type {
    optional_type {
      item {
        type_id: INT32
      }
    }
  }
}
columns {
  name: "int64"
  type {
    optional_type {
      item {
        type_id: INT64
      }
    }
  }
}
columns {
  name: "objectid"
  type {
    optional_type {
      item {
        type_id: STRING
      }
    }
  }
}
columns {
  name: "string"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
rows {
  items {
    int32_value: 0
  }
  items {
    bytes_value: "\252\252"
  }
  items {
    bool_value: true
  }
  items {
    double_value: 1.22
  }
  items {
    int32_value: 42
  }
  items {
    int64_value: 23423
  }
  items {
    bytes_value: "171e75500ecde1c75c59139e"
  }
  items {
    text_value: "hello"
  }
}
rows {
  items {
    int32_value: 1
  }
  items {
    bytes_value: "\253\253"
  }
  items {
    bool_value: false
  }
  items {
    double_value: 1.23
  }
  items {
    int32_value: 13
  }
  items {
    int64_value: 13
  }
  items {
    bytes_value: "271e75500ecde1c75c59139e"
  }
  items {
    text_value: "hi"
  }
}
rows {
  items {
    int32_value: 2
  }
  items {
    bytes_value: "\254\254"
  }
  items {
    bool_value: false
  }
  items {
    double_value: 1.24
  }
  items {
    int32_value: 15
  }
  items {
    int64_value: 15
  }
  items {
    bytes_value: "371e75500ecde1c75c59139e"
  }
  items {
    text_value: "bye"
  }
}
```

#### Запрос с представлением документа в формате JSON

Настройки внешнего источника данных с `reading_mode` = `JSON`.

Такой формат представления возрвращает две колонки - первичный ключ `_id` и JSON-строку, соответствующую всему документу. Поддерживается пушдаун фильтров по первичному ключу.
Для сериализации используется нативный для MongoDB формат [Extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/), который умеет представлять все типы данных BSON.


json_schema.yql
```sql
CREATE OBJECT mongodb_local_password (TYPE SECRET) WITH (value = "password");

CREATE EXTERNAL DATA SOURCE mongodb_external_datasource WITH (
    SOURCE_TYPE="MongoDB",
    LOCATION="localhost:27017",
    AUTH_METHOD="BASIC",
    LOGIN="admin",
    DATABASE_NAME="connector",
    PASSWORD_SECRET_NAME="mongodb_local_password",
    READING_MODE="JSON",
    UNSUPPORTED_TYPE_DISPLAY_MODE="UNSUPPORTED_OMIT",
    UNEXPECTED_TYPE_DISPLAY_MODE="UNEXPECTED_AS_NULL"
);
```

Файл с телом запроса

data.yql
```sql
SELECT * FROM mongodb_external_datasource.primitives;
```

Команда запроса

```sh
./ydb/tests/tools/kqprun/kqprun -s ydb/json_schema.yql -p ydb/data.yql --app-config=ydb/app_conf.txt --result-format="full-proto"
```

Вывод результата выполнения команды без логов:

```
columns {
  name: "_id"
  type {
    optional_type {
      item {
        type_id: INT32
      }
    }
  }
}
columns {
  name: "primitives"
  type {
    type_id: JSON
  }
}
rows {
  items {
    int32_value: 0
  }
  items {
    text_value: "{\"_id\":0,\"binary\":{\"Subtype\":0,\"Data\":\"qqo=\"},\"boolean\":true,\"double\":1.22,\"int32\":42,\"int64\":23423,\"objectid\":\"171e75500ecde1c75c59139e\",\"string\":\"hello\"}"
  }
}
rows {
  items {
    int32_value: 1
  }
  items {
    text_value: "{\"_id\":1,\"binary\":{\"Subtype\":0,\"Data\":\"q6s=\"},\"boolean\":false,\"double\":1.23,\"int32\":13,\"int64\":13,\"objectid\":\"271e75500ecde1c75c59139e\",\"string\":\"hi\"}"
  }
}
rows {
  items {
    int32_value: 2
  }
  items {
    text_value: "{\"_id\":2,\"binary\":{\"Subtype\":0,\"Data\":\"rKw=\"},\"boolean\":false,\"double\":1.24,\"int32\":15,\"int64\":15,\"objectid\":\"371e75500ecde1c75c59139e\",\"string\":\"bye\"}"
  }
}
```

#### Запрос с представлением документа в формате YSON

Настройки внешнего источника данных с `reading_mode` = `YSON`.

Такой формат представления возрвращает две колонки - первичный ключ `_id` и YSON сериализацию, соответствующую всему документу. Поддерживается пушдаун фильтров по первичному ключу.

yson_schema.yql
```sql
CREATE OBJECT mongodb_local_password (TYPE SECRET) WITH (value = "password");

CREATE EXTERNAL DATA SOURCE mongodb_external_datasource WITH (
    SOURCE_TYPE="MongoDB",
    LOCATION="localhost:27017",
    AUTH_METHOD="BASIC",
    LOGIN="admin",
    DATABASE_NAME="connector",
    PASSWORD_SECRET_NAME="mongodb_local_password",
    READING_MODE="YSON",
    UNSUPPORTED_TYPE_DISPLAY_MODE="UNSUPPORTED_OMIT",
    UNEXPECTED_TYPE_DISPLAY_MODE="UNEXPECTED_AS_NULL"
);
```

Файл с телом запроса

data.yql
```sql
SELECT * FROM mongodb_external_datasource.primitives;
```

Команда запроса

```sh
./ydb/tests/tools/kqprun/kqprun -s ydb/yson_schema.yql -p ydb/data.yql --app-config=ydb/app_conf.txt --result-format="full-proto"
```

Вывод результата выполнения команды без логов:

```
columns {
  name: "_id"
  type {
    optional_type {
      item {
        type_id: INT32
      }
    }
  }
}
columns {
  name: "primitives"
  type {
    type_id: YSON
  }
}
rows {
  items {
    int32_value: 0
  }
  items {
    bytes_value: "{\"_id\"=0;binary={Subtype=0u;Data=\"\\xAA\\xAA\";};boolean=%true;double=1.220000;int32=42;int64=23423;objectid=\"171e75500ecde1c75c59139e\";string=hello;}"
  }
}
rows {
  items {
    int32_value: 1
  }
  items {
    bytes_value: "{\"_id\"=1;binary={Subtype=0u;Data=\"\\xAB\\xAB\";};boolean=%false;double=1.230000;int32=13;int64=13;objectid=\"271e75500ecde1c75c59139e\";string=hi;}"
  }
}
rows {
  items {
    int32_value: 2
  }
  items {
    bytes_value: "{\"_id\"=2;binary={Subtype=0u;Data=\"\\xAC\\xAC\";};boolean=%false;double=1.240000;int32=15;int64=15;objectid=\"371e75500ecde1c75c59139e\";string=bye;}"
  }
}
```

#### Запрос c представлением ObjectId как YQL TaggedType<"ObjectId", String>

Меняем представление типа ObjectId на YQL TaggedType<"ObjectId", String> в настройках внешнего источника данных в конфиге fq-connector-go:

```yaml
mongodb:
  object_id_yql_type: OBJECT_ID_AS_TAGGED_STRING
  ...
```

Файл с телом запроса

bar.yql
```sql
SELECT * FROM mongodb_external_datasource.bar;
```

Команда запроса

```sh
./ydb/tests/tools/kqprun/kqprun -s ydb/schema.yql -p ydb/bar.yql --app-config=ydb/app_conf.txt --result-format="full-proto"
```

Вывод результата выполнения команды без логов:

```
columns {
  name: "_id"
  type {
    tagged_type {
      tag: "ObjectId"
      type {
        type_id: STRING
      }
    }
  }
}
columns {
  name: "a"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
columns {
  name: "b"
  type {
    optional_type {
      item {
        type_id: INT32
      }
    }
  }
}
columns {
  name: "c"
  type {
    optional_type {
      item {
        type_id: INT64
      }
    }
  }
}
rows {
  items {
    bytes_value: "67f3c139171bfd11df51e944"
  }
  items {
    text_value: "jelly"
  }
  items {
    int32_value: 2000
  }
  items {
    int64_value: 13
  }
}
rows {
  items {
    bytes_value: "67f3c139171bfd11df51e945"
  }
  items {
    text_value: "butter"
  }
  items {
    int32_value: -20021
  }
  items {
    int64_value: 0
  }
}
rows {
  items {
    bytes_value: "67f3c139171bfd11df51e946"
  }
  items {
    text_value: "toast"
  }
  items {
    int32_value: 2076
  }
  items {
    int64_value: 2076
  }
}
```

### Запросы с фильтрами

#### Запрос с фильтрацией по первичному ключу типа ObjectId

##### В случае представления ObjectId как YQL String (используется по умолчанию)

Настройки внешнего источника данных в конфиге fq-connector-go:

```yaml
mongodb:
  object_id_yql_type: OBJECT_ID_AS_STRING
  ...
```

Файл с телом запроса

bar.yql
```sql
SELECT * FROM mongodb_external_datasource.bar WHERE _id = '67f3c139171bfd11df51e944';
```

Команда запроса

```sh
./ydb/tests/tools/kqprun/kqprun -s ydb/schema.yql -p ydb/bar.yql --app-config=ydb/app_conf.txt --result-format="full-proto"
```

Вывод результата выполнения команды без логов:

```
columns {
  name: "_id"
  type {
    optional_type {
      item {
        type_id: STRING
      }
    }
  }
}
columns {
  name: "a"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
columns {
  name: "b"
  type {
    optional_type {
      item {
        type_id: INT32
      }
    }
  }
}
columns {
  name: "c"
  type {
    optional_type {
      item {
        type_id: INT64
      }
    }
  }
}
rows {
  items {
    bytes_value: "67f3c139171bfd11df51e944"
  }
  items {
    text_value: "jelly"
  }
  items {
    int32_value: 2000
  }
  items {
    int64_value: 13
  }
}
```

##### В случае представления ObjectId как YQL TaggedType<"ObjectId", String>

Настройки внешнего источника данных в конфиге fq-connector-go:

```yaml
mongodb:
  object_id_yql_type: OBJECT_ID_AS_TAGGED_STRING
  ...
```

Файл с телом запроса

bar.yql
```sql
SELECT * FROM mongodb_external_datasource.bar WHERE _id = AsTagged('67f3c139171bfd11df51e944', 'ObjectId');
```

Команда запроса

```sh
./ydb/tests/tools/kqprun/kqprun -s ydb/schema.yql -p ydb/bar.yql --app-config=ydb/app_conf.txt --result-format="full-proto"
```

Вывод результата выполнения команды без логов:

```
columns {
  name: "_id"
  type {
    optional_type {
      item {
        tagged_type {
          tag: "ObjectId"
          type {
            type_id: STRING
          }
        }
      }
    }
  }
}
columns {
  name: "a"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
columns {
  name: "b"
  type {
    optional_type {
      item {
        type_id: INT32
      }
    }
  }
}
columns {
  name: "c"
  type {
    optional_type {
      item {
        type_id: INT64
      }
    }
  }
}
rows {
  items {
    bytes_value: "67f3c139171bfd11df51e944"
  }
  items {
    text_value: "jelly"
  }
  items {
    int32_value: 2000
  }
  items {
    int64_value: 13
  }
}
```

#### Запрос с оператором LIKE

Файл с телом запроса

bar.yql
```sql
SELECT * FROM mongodb_external_datasource.bar WHERE a LIKE 'toast';
```

Команда запроса

```sh
./ydb/tests/tools/kqprun/kqprun -s ydb/schema.yql -p ydb/bar.yql --app-config=ydb/app_conf.txt --result-format="full-proto"
```

Вывод результата выполнения команды без логов:

```
columns {
  name: "_id"
  type {
    optional_type {
      item {
        type_id: STRING
      }
    }
  }
}
columns {
  name: "a"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
columns {
  name: "b"
  type {
    optional_type {
      item {
        type_id: INT32
      }
    }
  }
}
columns {
  name: "c"
  type {
    optional_type {
      item {
        type_id: INT64
      }
    }
  }
}
rows {
  items {
    bytes_value: "67f3c139171bfd11df51e946"
  }
  items {
    text_value: "toast"
  }
  items {
    int32_value: 2076
  }
  items {
    int64_value: 2076
  }
}
```
