# Реализация коннектора к системе мониторинга Prometheus

## Trino + Presto _([docs](https://trino.io/docs/current/connector/prometheus.html#prometheus-connector))_

### Схема таблиц в Trino + Presto

Для каждой метрики (а именно ее названия `__name__`) создается своя таблица в Trino, которая ВСЕГДА имеет следующую структуру:

| labels                   | timestamp                     | value    |
|--------------------------|-------------------------------|----------|
| _map(varchar, varchar)_  | _timestamp(3) with time zone_ | _double_ |

### Маппинг типов данных

Маппинг данных выполняется по таблице:

| Prometheus          | Тип Trino                    |
|---------------------|------------------------------|
| _labels_            | _MAP(VARCHAR,VARCHAR)_       |
| _TIMESTAMP_         | _TIMESTAMP(3) WITH TIMEZONE_ |
| _value_             | _DOUBLE_                     |

**Никакие другие типы не поддерживаются**

### Поддерживаемые операции

_The connector provides **globally available** and **read operation** statements to access data and metadata in Prometheus._

Из интересующих нас операций только `SELECT` с поддержкой match. _([SQL support](https://trino.io/docs/current/connector/prometheus.html#sql-support))_

### Пример интеграции

В примере ниже мы запустили окружение, состоящее из следующих компонентов:

- Prometheus
- Два простейших сервера на Go (`echo` и `fasthttp`) с настроенным экспортом метрик в Prometheus для генерации данных
- Trino

После чего подключились к Trino и при помощи его CLI выполнили следующие команды:

- `use prometheus.default;` - выбрали схему и каталог для работы
- `describe up;` - получили структуру таблицы для метрики с названием `up`
- `show tables like 'go_mem%';` - получили все таблицы с метриками, название которых начинается на `go_mem` (_в которых расположены метрики по использованию памяти нашими серверами, `__name__ like 'go_mem%'`_)
- `select count(*) from up;` - получили кол-во метрик в таблице `up`
- `select * from up limit 10;` - получили 10 метрик из таблицы `up`

```shell
╰─➤  make build-env
docker build -t go_env:latest . -f env.dockerfile

╰─➤  make up 
docker compose up -d
[+] Running 5/5
 ✔ Network trino_default    Created
 ✔ Container prometheus     Started
 ✔ Container fasthttp-ping  Started
 ✔ Container echo-ping      Started
 ✔ Container trino          Started

╰─➤  docker exec -it trino bash
[trino@3fa6aa130927 /]$ cd bin/ && ./trino
trino> use prometheus.default;
USE
trino:default> describe up;
  Column   |            Type             | Extra | Comment 
-----------+-----------------------------+-------+---------
 labels    | map(varchar, varchar)       |       |         
 timestamp | timestamp(3) with time zone |       |         
 value     | double                      |       |         
(3 rows)

trino:default> show tables like 'go_mem%';
              Table               
----------------------------------
 go_memstats_alloc_bytes          
 go_memstats_alloc_bytes_total     
 ...       
 go_memstats_stack_inuse_bytes    
 go_memstats_stack_sys_bytes      
 go_memstats_sys_bytes            
(22 rows)

trino:default> select count(*) from up;
 _col0 
-------
   440 
(1 row)

trino:default> select * from up limit 10;
                        labels                         |          timestamp          | value 
-------------------------------------------------------+-----------------------------+-------
 {instance=echo-ping:8081, __name__=up, job=echo-ping} | 2025-03-08 09:25:38.742 UTC |   0.0 
 {instance=echo-ping:8081, __name__=up, job=echo-ping} | 2025-03-08 09:25:39.744 UTC |   0.0 
 {instance=echo-ping:8081, __name__=up, job=echo-ping} | 2025-03-08 09:25:40.742 UTC |   1.0 
 {instance=echo-ping:8081, __name__=up, job=echo-ping} | 2025-03-08 09:25:41.742 UTC |   1.0 
 {instance=echo-ping:8081, __name__=up, job=echo-ping} | 2025-03-08 09:25:42.742 UTC |   1.0 
 {instance=echo-ping:8081, __name__=up, job=echo-ping} | 2025-03-08 09:25:43.742 UTC |   1.0 
 {instance=echo-ping:8081, __name__=up, job=echo-ping} | 2025-03-08 09:25:44.742 UTC |   1.0 
 {instance=echo-ping:8081, __name__=up, job=echo-ping} | 2025-03-08 09:25:45.742 UTC |   1.0 
 {instance=echo-ping:8081, __name__=up, job=echo-ping} | 2025-03-08 09:25:46.748 UTC |   1.0 
 {instance=echo-ping:8081, __name__=up, job=echo-ping} | 2025-03-08 09:25:47.742 UTC |   1.0 
(10 rows)
```

## Postgres

Платная интеграция _([prometheus-fdw](https://tembo.io/blog/monitoring-with-prometheus-fdw))_

## Amazon Athena

Платная интеграция _([tray.ai](https://tray.ai/connectors/amazon-athena-prometheus-integrations))_

## ClickHouse

Только экспорт метрик из ClickHouse в Prometheus _([docs](https://clickhouse.com/docs/integrations/prometheus))_

## Azure Data Explorer

Адаптер от ДоДо пиццы (ничего не понятно, надо ли) _[DoDo](https://github.com/dodopizza/Prometheus-AzureDataExplorer?tab=readme-ov-file)_

## Реализация коннектора в YDB

### ПсевдоMoSCoW

Минимум:
- `SELECT * FROM ... ` для конкретной метрики и парсинг всех `label` в отдельный столбец
- Поддержка маппинга типов данных, описанного ниже
- `LIMIT`, `OFFSET` - на стороне коннектора
- Пушдаун фильтров: операторов сравнения, логических операторов, матчинг `label` с `LIKE`, `ORDER BY`

Продвинутая реализация
- `LIMIT`, `OFFSET` - на стороне Prometheus, если значение задано временем (например, `LIMIT 5m OFFSET 1w`)
- Добавление функций над временными рядами в YDB, чтобы выполнять сложные функции на стороне Prometheus

### Схема таблицы

Результатом `SELECT *` запроса всегда будет таблица следующего вида:

| Тип метрики | Название метрики (`__name__`) | ...      | Лэйблы (`label`) | ...      | Время (`timestamp`) | Значение (`value`)                                                                                  |
|-------------|-------------------------------|----------|------------------|----------|---------------------|-----------------------------------------------------------------------------------------------------|
| `String`    | `String`                      | `String` | `String`         | `String` | `Timestamp`         | `Decimal` \| `List<Decimal>` \| `Dict<Decimal, Uint64 \| Decimal \| List<Uint64> \| List<Decimal>>` |

**Колонка "Значение (`value`)" может содержать разные типы данных, зависимость описана в разделе ниже**

### Колонка `value` результирующей таблицы YDB

В таблице ниже описана зависимость типы данных в результирующей таблице YDB от типа метрики и типа данных Prometheus (от простейшего запроса с простейшими метриками)

| Тип метрики Prometheus | Тип данных Prometheus | Пример запроса Prometheus              | YDB                           | Комментарий                                                                                                         |
|------------------------|-----------------------|----------------------------------------|-------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `counter`              | `Instant vector`      | `echo_requests_total`                  | `Decimal`                     | `echo_requests_total` - `counter` метрика                                                                           |
| `counter`              | `Range vector`        | `echo_requests_total[10s]`             | `List<Decimal>`               | `List<10 значений метрики из предыдущих 10 секунд>`                                                                 |
| `gauge`                | `Instant vector`      | `go_memstats_sys_bytes`                | `Decimal`                     | `go_memstats_sys_bytes` - `gauge` метрика                                                                           |
| `gauge`                | `Range vector`        | `go_memstats_sys_bytes[10s]`           | `List<Decimal>`               | `List<10 значений метрики из предыдущих 10 секунд>`                                                                 |
| `histogram`            | `Instant vector`      | `echo_response_size_bytes_bucket`      | `Dict<Decimal, Uint64>`       | `echo_response_size_bytes_bucket` - `histogram` метрика; <br/><br/> `Dict<значение le (<=), кол-во значений <= le>` |
| `histogram`            | `Range vector`        | `echo_response_size_bytes_bucket[10s]` | `Dict<Decimal, List<Uint64>>` | `Dict<значение le (<=), List<10 значений кол-ва предыдущих 10 секунд>>`                                             |
| `summary`              | `Instant vector`      | `go_gc_duration_seconds`               | `Dict<Float, Decimal>`        | `go_gc_duration_seconds` - `summary` метрика; <br/><br/> `Dict<уровень quantile (от 0 до 1), значение quantile>`    |
| `summary`              | `Range vector`        | `go_gc_duration_seconds[10s]`          | `Dict<Float, List<Decimal>>`  | `Dict<уровень quantile (от 0 до 1), List<значения 10 квантилей предыдущих 10 секунд>>`                              |
