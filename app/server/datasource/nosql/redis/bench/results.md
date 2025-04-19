# Как проводились бенчмарки

- на ВМ без лишних процессов был запущен контейнер с Redis из
  файла [docker-compose.yaml](../../../../../../scripts/bench/docker-compose.yaml)
- затем Redis был заполнен тестовыми данными с помощью
  скрипта [prepareredis.go](../../../../../../scripts/bench/prepareredis.go)
- после чего из корня репозитория выполняем команду
    ```bash
    make build && ./fq-connector-go bench ./scripts/bench/rediscolumns.txt 
    ```
- затем для оценки задержек, вызванных коннектором, проводим бенчмарк максимально простого приложения, выполняющего
  чтение и преобразования аналогичные логике в коннекторе [simpleapp.go](simpleapp.go)

## Результаты

| приложение      | режим  | длина строки | ключей в hash | MB/s   | строк/сек |
|-----------------|--------|--------------|---------------|--------|-----------|
| simple          | string | 10           |               | 5.36   | 258172    |
| fq-connector-go | string | 10           |               | 5.18   | 249433    |
| simple          | string | 128          |               | 27.08  | 220000    |
| fq-connector-go | string | 128          |               | 31.18  | 235460    |
| simple          | string | 1024         |               | 165.94 | 168147    |
| fq-connector-go | string | 1024         |               | 147.31 | 149275    |
| simple          | string | 2048         |               | 274.84 | 139994    |
| fq-connector-go | string | 2048         |               | 223.98 | 114090    |
| simple          | string | 4096         |               | 384.57 | 98195     |
| fq-connector-go | string | 4096         |               | 284    | 72536     |
| simple          | hash   | 10           | 10            | 25.32  | 146654    |
| fq-connector-go | hash   | 10           | 10            | 21.54  | 124714    |
| simple          | hash   | 100          | 10            | 90.41  | 87716     |
| fq-connector-go | hash   | 100          | 10            | 87.56  | 84953     |
