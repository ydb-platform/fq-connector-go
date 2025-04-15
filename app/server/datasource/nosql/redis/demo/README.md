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
SET sample_session:3 ghi
SET sample_session:4 jkl
SET sample_session:5 mno

HMSET sample_session:112233445 email "mia@example.com" last_activity "2023-01-11 21:00:00" user_id "112" username "mia_evans"
HMSET sample_session:334455667 email "ethan@example.com" last_activity "2023-01-12 22:15:00" user_id "334" username "ethan_white"
HMSET sample_session:567890123 email "emily@example.com" last_activity "2023-01-05 13:30:00" user_id "567" username "emily_brown"
HMSET sample_session:990011223 email "mia@example.com" last_activity "2023-01-15 02:00:00" user_id "990" username "mia_thompson"
HMSET sample_session:890123456 user_id "890" username "david_wilson"
HMSET sample_session:234567890 email "jane@example.com" last_activity "2023-01-02 09:45:00" user_id "234" username "jane_smith"
HMSET sample_session:789012345 email "sophia@example.com" last_activity "2023-01-07 16:00:00" user_id "789" username "sophia_taylor"
HMSET sample_session:678901234 email "chris@example.com" last_activity "2023-01-06 14:45:00" user_id "678" username "chris_black"
HMSET sample_session:012345678 email "noah@example.com" last_activity "2023-01-10 19:45:00" user_id "012" username "noah_hall"
HMSET sample_session:456789012 email "bob@example.com" last_activity "2023-01-04 12:15:00" user_id "456" username "bob_jones"
HMSET sample_session:556677889 user_id "556" username "ava_martin"
HMSET sample_session:123456789 email "john@example.com" last_activity "2023-01-01 08:30:00" user_id "123" username "john_doe"
HMSET sample_session:901234567 email "olivia@example.com" last_activity "2023-01-09 18:30:00" user_id "901" username "olivia_lee"
HMSET sample_session:778899001 user_id "778" username "logan_anderson"
HMSET sample_session:345678901 email "alice@example.com" last_activity "2023-01-03 11:00:00" user_id "345" username "alice_green"
```

3. Запускаем YDB Embedded UI:

```bash
./ydb/tests/tools/kqprun/kqprun 
    --app-config=/home/vitalyisaev/arcadia/junk/vitalyisaev/connectors/app_config.local.conf  
    -M 8080
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