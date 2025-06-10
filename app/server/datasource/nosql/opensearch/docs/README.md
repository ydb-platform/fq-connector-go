# Демо поддержки федеративных запросов к OpenSearch из локальной инсталяции YDB

## Подготовка данных во внешнем источнике данных OpenSearch

Поднимаем кастомный образ OpenSearch в корневой директории кода коннектора fq-connector-go

```sh
sudo docker compose -f ./tests/infra/datasource/docker-compose.yaml build --no-cache opensearch
sudo docker compose -f ./tests/infra/datasource/docker-compose.yaml up -d opensearch
```

Далее добавляем в OpenSearch индекс и документы. Они автоматически добавляются в volume
из скрипта `./tests/infra/datasource/opensearch/init/opensearch-init.sh`.
Cм. пример ниже.

```sh
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
```

Запускаем коннектор из корневой директории fq-connector-go.

```sh
make build
make run
```

## Проверка федеративных запросов к OpenSearch на стороне коннектора

Для проверки работспособности федеративных запросов на стороне клиента коннектора
создадим конфигурационный файл для настройки ` scripts/debug/config/client/opensearch.local.txt`.
См. пример ниже.

```
connector_server_endpoint {
    host: "localhost"
    port: 2130
}

metrics_server_endpoint {
    host: "localhost"
    port: 8766
}

data_source_instance {
    kind: OPENSEARCH
    endpoint {
        host: "localhost"
        port: 9200
    }
    database: "connector"
    credentials {
        basic {
            username: "admin"
            password: "password"
        }
    }
    protocol: HTTP
}
```

Далее запустим тестовый клиент `./fq-connector-go`
из корневой директории, обратившись к таблице `simple`.

```shell
 ./fq-connector-go client connector read_table --config scripts/debug/config/client/opensearch.local.txt --table nested
```

В логах увидим корректно полученные данные.

```shell
2025-05-17T20:06:50.333+0300    DEBUG   connector/read_table.go:210     column  {"data_source_kind": "OPENSEARCH", "database": "connector", "host": "localhost", "port": 9200, "protocol": "HTTP", "id": 0, "column_name": "_id", "data": "[\"0\" \"1\"]"}
2025-05-17T20:06:50.333+0300    DEBUG   connector/read_table.go:210     column  {"data_source_kind": "OPENSEARCH", "database": "connector", "host": "localhost", "port": 9200, "protocol": "HTTP", "id": 1, "column_name": "name", "data": "[\"Alice\" \"Bob\"]"}
2025-05-17T20:06:50.333+0300    DEBUG   connector/read_table.go:210     column  {"data_source_kind": "OPENSEARCH", "database": "connector", "host": "localhost", "port": 9200, "protocol": "HTTP", "id": 2, "column_name": "nested", "data": "{[\"SGVsbG8gQWxpY2U=\" \"SGVsbG8gQm9i\"] [1 0] [3.1415926535912346 2.7182818284512344] [3.14 2.71] [42 24] [1234567890123 9876543210987] [\"value1\" \"value2\"] [1689854400000000 1689953400000000]}"}
```

## Проверка федеративных запросов к OpenSearch из YDB

Для проверки работоспособности федеративных запросов к OpenSearch из YDB
нужно воспользоваться клиентом kqprun по [инструкции](https://github.com/ydb-platform/fq-connector-go/blob/main/docs/contribution.md#%D0%BF%D0%BE%D0%B4%D0%B4%D0%B5%D1%80%D0%B6%D0%BA%D0%B0-%D0%BD%D0%BE%D0%B2%D0%BE%D0%B3%D0%BE-%D0%B8%D1%81%D1%82%D0%BE%D1%87%D0%BD%D0%B8%D0%BA%D0%B0-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%B2-ydb).

### Конфигурация данных для выполнения запроса

schema.yql
```sql
CREATE OBJECT opensearch_local_password (TYPE SECRET) WITH (value = "password");

CREATE EXTERNAL DATA SOURCE external_datasource WITH (
    SOURCE_TYPE="OpenSearch",
    LOCATION="localhost:9200",
    AUTH_METHOD="BASIC",
    PROTOCOL="HTTP",
    LOGIN="admin",
    DATABASE_NAME="connector",
    PASSWORD_SECRET_NAME="opensearch_local_password",
    USE_TLS="false"
);
```

opensearch_conf.txt
```
FeatureFlags {
    EnableExternalDataSources: true
    EnableScriptExecutionOperations: true
}

QueryServiceConfig {
    AvailableExternalDataSources: "OpenSearch"
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

### Запрос SELECT * в индекс `simple`


Файл с телом запроса


data.sql
```sql
SELECT * FROM external_datasource.`simple`;
```

Команда запроса

```sh
./kqprun -s schema.yql -p data.yql --app-config=opensearch_conf.txt  --result-format="full-proto"
```

Вывод результата выполнения команды:

```
columns {
  name: "_id"
  type {
    type_id: UTF8
  }
}
columns {
  name: "bool_field"
  type {
    optional_type {
      item {
        type_id: BOOL
      }
    }
  }
}
columns {
  name: "double_field"
  type {
    optional_type {
      item {
        type_id: DOUBLE
      }
    }
  }
}
columns {
  name: "float_field"
  type {
    optional_type {
      item {
        type_id: FLOAT
      }
    }
  }
}
columns {
  name: "int32_field"
  type {
    optional_type {
      item {
        type_id: INT32
      }
    }
  }
}
columns {
  name: "int64_field"
  type {
    optional_type {
      item {
        type_id: INT64
      }
    }
  }
}
columns {
  name: "string_field"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
columns {
  name: "timestamp_field"
  type {
    optional_type {
      item {
        type_id: TIMESTAMP
      }
    }
  }
}
rows {
  items {
    text_value: "0"
  }
  items {
    bool_value: true
  }
  items {
    double_value: 2.71828
  }
  items {
    float_value: 1.5
  }
  items {
    int32_value: 42
  }
  items {
    int64_value: 1234567890123
  }
  items {
    text_value: "text_value1"
  }
  items {
    uint64_value: 1672531200000000
  }
}
rows {
  items {
    text_value: "1"
  }
  items {
    bool_value: false
  }
  items {
    double_value: 0
  }
  items {
    float_value: -3.14
  }
  items {
    int32_value: -100
  }
  items {
    int64_value: -987654321
  }
  items {
    text_value: "text_value2"
  }
  items {
    uint64_value: 1676462400000000
  }
}
rows {
  items {
    text_value: "2"
  }
  items {
    bool_value: true
  }
  items {
    double_value: -1.2345
  }
  items {
    float_value: 0
  }
  items {
    int32_value: 0
  }
  items {
    int64_value: 0
  }
  items {
    text_value: "text_value3"
  }
  items {
    uint64_value: 1679337000000000
  }
}
```

### Запрос с проекцией и пушдауном предикатов в индекс `simple`

Файл с телом запроса

data.yql
```sql
SELECT `_id`, `bool_field` FROM external_datasource.`simple` WHERE `bool_field`=true;
```


Вывод результата выполнения команды:
```
columns {
  name: "_id"
  type {
    type_id: UTF8
  }
}
columns {
  name: "bool_field"
  type {
    optional_type {
      item {
        type_id: BOOL
      }
    }
  }
}
rows {
  items {
    text_value: "0"
  }
  items {
    bool_value: true
  }
}
rows {
  items {
    text_value: "2"
  }
  items {
    bool_value: true
  }
}
```

### Запрос с проекцией и пушдауном REGEXP в индекс `simple`


Файл с телом запроса

data.yql
```sql
SELECT `_id`, `string_field` FROM external_datasource.`simple` WHERE `string_field` REGEXP 'text_value[1-2]';
```

```
columns {
  name: "_id"
  type {
    type_id: UTF8
  }
}
columns {
  name: "string_field"
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
    text_value: "0"
  }
  items {
    text_value: "text_value1"
  }
}
rows {
  items {
    text_value: "1"
  }
  items {
    text_value: "text_value2"
  }
```