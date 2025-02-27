# Docker compose setup for Trino + Redis/Valkey

## Official Docs:
- [Trino](https://trino.io/docs/current/connector/redis.html)
- [Valkey (open-source Redis)](https://valkey.io/download/)

## Project Structure

```
./
├──── data/
│     └── ... # empty folder for redis usage
├──── etc/
│     └── catalog/
│        └── redis.properties
│     └── table-descriptions/
│         └── example-table.json
│     └── config.properties
│     └── log.properties
│     └── node.properties
│     └── jvm.config
├── docker-compose.yml
└── readme.md
```

## Instructions

### Steps to Run

1. Pull containers:

```sh
docker pull trinodb/trino
docker pull valkey/valkey
```

2. Start the services:

```sh
docker-compose up -d
```

### Example Usage

1. Connect to Valkey:

```sh
docker exec -it valkey redis-cli
```

2. Insert some data:

```r
HSET example_table:1 field1 "Alice" field2 "Wonderland" field3 "25";
```

3. Connect to Trino:

```sh
docker exec -it trino trino
```

4. Run a sample query:

```sql
SELECT * FROM redis.default.example_table;
```

You should see this:

```
id        | field1 |   field2    | field3 
-----------------+--------+-------------+--------
 example_table:2 | Bob    | Builderland | 30     
 example_table:1 | Alice  | Wonderland  | 25     
(2 rows)

Query 20250105_174102_00000_nf6v2, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.40 [2 rows, 0B] [5 rows/s, 0B/s]
```