1. Подъём Redis + Redis UI

```
cd app/server/datasource/nosql/redis/demo
docker compose up -d
docker exec -it redis redis-cli --pass password
```

2. Вставляем данные

```
SET sample_session:1 abc
SET sample_session:2 def
SET sample_session:5 ghi
HMSET sample_session:990011223 email "mia@example.com" last_activity "2023-01-15 02:00:00" user_id "990" username "mia_thompson"
HMSET sample_session:890123456 user_id "890" username "david_wilson"
HMSET sample_session:234567890 email "jane@example.com" last_activity "2023-01-02 09:45:00" user_id "234" username "jane_smith"
```

3. Запускаем YDB Embedded UI:

```bash
./ydb/tests/tools/kqprun/kqprun --app-config=/home/glebbs/ydb/ydb/tests/tools/kqprun/configuration/app_config.conf -M 8080
```

4. Интерфейсы:

* [Redis UI](http://localhost:8081/)
* [YDB Embedded UI](http://localhost:8080/monitoring/tenant?tenantPage=query&database=%2FRoot&schema=%2FRoot%2Fexternal_data_source)

5. Запросы YQL через Embedded UI:

```sql
CREATE OBJECT secret_password (TYPE SECRET) WITH (value = "password");


CREATE EXTERNAL DATA SOURCE external_data_source WITH (
    SOURCE_TYPE="Redis",
    LOCATION="localhost:6379",
    DATABASE_NAME="0",
    AUTH_METHOD="BASIC",
    LOGIN="default",
    PASSWORD_SECRET_NAME="secret_password",
    USE_TLS="FALSE"
);
```

```sql
-- Прочитать все ключи
SELECT * FROM external_data_source.`*`

-- Прочитать все ключи, начинающиеся на sample_session:
SELECT * FROM external_data_source.`sample_session:*`
```

6. Запросы YQL через kqprun CLI:

```bash
./ydb/tests/tools/kqprun/kqprun 
    -s /home/vitalyisaev/projects/fq-connector-go/scripts/debug/kqprun/schema.redis.local.txt 
    -p /home/vitalyisaev/projects/fq-connector-go/scripts/debug/kqprun/script.redis.local.txt 
    --app-config=/home/vitalyisaev/arcadia/junk/vitalyisaev/connectors/app_config.local.conf  
    --result-format=full-proto
```
