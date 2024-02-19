![Build](https://github.com/ydb-platform/fq-connector-go/actions/workflows/build.yml/badge.svg)
![Linter](https://github.com/ydb-platform/fq-connector-go/actions/workflows/lint.yml/badge.svg)
[![Coverage](https://codecov.io/github/ydb-platform/fq-connector-go/graph/badge.svg?token=RCXN9X391Y)](https://codecov.io/github/ydb-platform/fq-connector-go)

### Objectives

Service `fq-connector-go` is a part of YDB Federative Query system.
It's an extension point making YDB capable of interaction with various external data sources.
You can deploy `fq-connector-go` alongside with YDB in order to query and join YDB tables 
with the data extracted from the external sources.

Currently supported data sources:
* ClickHouse
* PostgreSQL

### Usage 

Use this command to run Connector with [default configuration](https://github.com/ydb-platform/fq-connector-go/blob/main/example.conf):

```
docker run -d \
    --name=connector \
    -p 50051:50051 \
    -p 6060:6060 \
    ghcr.io/ydb-platform/fq-connector-go:latest
```

Or you can mount custom configuration into container:

```
docker run -d \
    --name=connector \
    -p 50051:50051 \
    -p 6060:6060 \
    -v /tmp/example.conf:/usr/local/etc/fq-connector-go.conf \
    ghcr.io/ydb-platform/fq-connector-go:latest
```

