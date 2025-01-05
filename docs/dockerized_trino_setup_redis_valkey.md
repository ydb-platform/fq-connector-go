# Docker compose setup for Trino + Redis/Valkey

## Official Documentation:
- [Trino](https://trino.io/docs/current/connector/redis.html)
- [Valkey (open-source Redis)](https://valkey.io/download/)

## Project Structure

```
./
├──── data/
│     └── ... # create empty folder for redis usage
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

## File Contents

### docker-compose.yml

```yaml
version: '3.8'

services:
  trino:
    image: trinodb/trino:latest
    container_name: trino
    ports:
      - "8080:8080"
    volumes:
      - ./etc:/etc/trino
      - ./data:/var/trino/data
    depends_on:
      - valkey

  valkey:
    image: valkey/valkey:8.0.1
    container_name: valkey
    ports:
      - "6379:6379"
```

### redis.properties

```properties
connector.name=redis
redis.table-names=example_table
redis.nodes=valkey:6379
redis.default-schema=default
redis.table-description-dir=/etc/trino/table-descriptions
```

### example_table.json

```properties
{
    "tableName": "example_table",
    "schemaName": "default",
    "key": {
        "dataFormat": "raw",
        "fields": [
            {
                "name": "id",
                "type": "varchar"
            }
        ]
    },
    "value": {
        "dataFormat": "hash",
        "fields": [
            {
                "name": "field1",
                "type": "varchar",
                "mapping": "field1"
            },
            {
                "name": "field2",
                "type": "varchar",
                "mapping": "field2"
            },
            {
                "name": "field3",
                "type": "varchar",
                "mapping": "field3"
            }
        ]
    }
}
```

### config.properties

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
```

### jvm.config

```properties
-server
-Xmx16G
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-Dfile.encoding=UTF-8
-XX:+EnableDynamicAgentLoading
```

### log.properties

```properties
io.trino=INFO
```

### log.properties

```properties
node.environment=production
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.data-dir=/var/trino/data
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