### Docker Compose setup for Trino (x MongoDB)

Пример настройки Trino локально с использованием MongoDB в качестве источника данных.

#### 0. Prep

```sh
docker pull trinodb/trino
docker pull mongo
```

- https://trino.io/docs/current/installation/containers.html
- https://hub.docker.com/r/trinodb/trino

#### 1. Trino configuration

- [Configuring Trino](https://trino.io/docs/current/installation/deployment.html#configuring-trino)
- [Community tutorials | github](https://github.com/bitsondatadev/trino-getting-started/tree/main/community-tutorials)

minimal setup:
```
.
├── docker-compose.yaml
├── etc                              # trino configuration volume
│   └── catalog
│       └── mongo.properties         # ext data source connection config
│   └── jvm.config
│   └── node.properties
│   └── config.properties
│   └── log.properties
└── mongodb                          # ext data source data volume
```

#### 2. Run & populate external data source

```
docker compose up -d

# find out container name with 'docker ps'
docker exec -it trino-mongo-mongodb-1 mongosh
mongosh > db.createCollection("orders");
mongosh > ...
```

#### 3. Query external data source from Trino

- [Trino CLI](https://docs.starburst.io/clients/cli.html#cli)

```
docker exec -it trino-mongo-trino-1 trino

trino > show catalogs;
mongo
trino > show schemas from mongo;
test
trino > show tables from mongo.test;
orders
trino > show columns from mongo.test.orders;
...
trino > select * from mongo.test.orders;
...
```
