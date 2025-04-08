### Подготавливаем данные в источнике:

- Запускаем Redis

    ```sh
    sudo docker compose -f ./tests/infra/datasource/docker-compose.yaml up -d redis
    ```

- Заходим в контейнер и выполняем команду:

    ```
    redis-cli
    ```

- Наполняем данными:

    ```
    HMSET sample_session:112233445 email "mia@example.com" last_activity "2023-01-11 21:00:00" user_id "112" username "mia_evans"
    HMSET sample_session:334455667 email "ethan@example.com" last_activity "2023-01-12 22:15:00" user_id "334" username "ethan_white"
    HMSET sample_session:567890123 email "emily@example.com" last_activity "2023-01-05 13:30:00" user_id "567" username "emily_brown"
    HMSET sample_session:990011223 email "mia@example.com" last_activity "2023-01-15 02:00:00" user_id "990" username "mia_thompson"
    HMSET sample_session:890123456 email "" last_activity "" user_id "890" username "david_wilson"
    HMSET sample_session:234567890 email "jane@example.com" last_activity "2023-01-02 09:45:00" user_id "234" username "jane_smith"
    HMSET sample_session:789012345 email "sophia@example.com" last_activity "2023-01-07 16:00:00" user_id "789" username "sophia_taylor"
    HMSET sample_session:678901234 email "chris@example.com" last_activity "2023-01-06 14:45:00" user_id "678" username "chris_black"
    HMSET sample_session:012345678 email "noah@example.com" last_activity "2023-01-10 19:45:00" user_id "012" username "noah_hall"
    HMSET sample_session:456789012 email "bob@example.com" last_activity "2023-01-04 12:15:00" user_id "456" username "bob_jones"
    HMSET sample_session:556677889 email "" last_activity "" user_id "556" username "ava_martin"
    HMSET sample_session:123456789 email "john@example.com" last_activity "2023-01-01 08:30:00" user_id "123" username "john_doe"
    HMSET sample_session:901234567 email "olivia@example.com" last_activity "2023-01-09 18:30:00" user_id "901" username "olivia_lee"
    HMSET sample_session:778899001 email "" last_activity "" user_id "778" username "logan_anderson"
    HMSET sample_session:345678901 email "alice@example.com" last_activity "2023-01-03 11:00:00" user_id "345" username "alice_green"
    
    SET sample_session:1 stringValue1
    SET sample_session:2 stringValue2
    ```

- Запускаем коннектор
    ```
    make build && make run 
    ```

### Готовим файлы для выполнения запросов:

- schema.yql
    ```
    CREATE OBJECT secret_password (TYPE SECRET) WITH (value = "");
    
    
    CREATE EXTERNAL DATA SOURCE external_data_source WITH (
        SOURCE_TYPE="Redis",
        LOCATION="localhost:6379",
        DATABASE_NAME="0",
        AUTH_METHOD="BASIC",
        LOGIN="default",
        PASSWORD_SECRET_NAME="secret_password",
        --PROTOCOL="NATIVE",
        USE_TLS="FALSE"
    );
    ```

- data.yql
    ```
    SELECT * FROM external_data_source.`sample_session:*`;
    ```

- app_conf.txt
    ```
    FeatureFlags {
        EnableExternalDataSources: true
        EnableScriptExecutionOperations: true
    }
    
    QueryServiceConfig {
        AvailableExternalDataSources: "Redis"
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

**Все файлы складываем в `./ydb/tests/tools/kqprun/configuration`**

### Делаем запрос в YDB:
```
cd ./ydb/tests/tools/kqprun
```

```
./kqprun -s configuration/schema.yql -p configuration/data.yql --app-config=configuration/app_conf.txt
```

### Видим вывод
```
{"hash_values":{"email":"mia@example.com","last_activity":"2023-01-11 21:00:00","user_id":"112","username":"mia_evans"},"key":"sample_session:112233445","string_values":null}
{"hash_values":{"email":"ethan@example.com","last_activity":"2023-01-12 22:15:00","user_id":"334","username":"ethan_white"},"key":"sample_session:334455667","string_values":null}
{"hash_values":{"email":"emily@example.com","last_activity":"2023-01-05 13:30:00","user_id":"567","username":"emily_brown"},"key":"sample_session:567890123","string_values":null}
{"hash_values":{"email":"mia@example.com","last_activity":"2023-01-15 02:00:00","user_id":"990","username":"mia_thompson"},"key":"sample_session:990011223","string_values":null}
{"hash_values":null,"key":"sample_session:1","string_values":"stringValue1"}
{"hash_values":{"email":null,"last_activity":null,"user_id":"890","username":"david_wilson"},"key":"sample_session:890123456","string_values":null}
{"hash_values":{"email":"jane@example.com","last_activity":"2023-01-02 09:45:00","user_id":"234","username":"jane_smith"},"key":"sample_session:234567890","string_values":null}
{"hash_values":{"email":"sophia@example.com","last_activity":"2023-01-07 16:00:00","user_id":"789","username":"sophia_taylor"},"key":"sample_session:789012345","string_values":null}
{"hash_values":{"email":"chris@example.com","last_activity":"2023-01-06 14:45:00","user_id":"678","username":"chris_black"},"key":"sample_session:678901234","string_values":null}
{"hash_values":{"email":"noah@example.com","last_activity":"2023-01-10 19:45:00","user_id":"012","username":"noah_hall"},"key":"sample_session:012345678","string_values":null}
{"hash_values":{"email":"bob@example.com","last_activity":"2023-01-04 12:15:00","user_id":"456","username":"bob_jones"},"key":"sample_session:456789012","string_values":null}
{"hash_values":{"email":null,"last_activity":null,"user_id":"556","username":"ava_martin"},"key":"sample_session:556677889","string_values":null}
{"hash_values":{"email":"john@example.com","last_activity":"2023-01-01 08:30:00","user_id":"123","username":"john_doe"},"key":"sample_session:123456789","string_values":null}
{"hash_values":{"email":"olivia@example.com","last_activity":"2023-01-09 18:30:00","user_id":"901","username":"olivia_lee"},"key":"sample_session:901234567","string_values":null}
{"hash_values":null,"key":"sample_session:2","string_values":"stringValue2"}
{"hash_values":{"email":null,"last_activity":null,"user_id":"778","username":"logan_anderson"},"key":"sample_session:778899001","string_values":null}
{"hash_values":{"email":"alice@example.com","last_activity":"2023-01-03 11:00:00","user_id":"345","username":"alice_green"},"key":"sample_session:345678901","string_values":null}
```
