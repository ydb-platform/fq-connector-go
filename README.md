![Build](https://github.com/ydb-platform/fq-connector-go/actions/workflows/build.yml/badge.svg)
![Linter](https://github.com/ydb-platform/fq-connector-go/actions/workflows/lint.yml/badge.svg)
[![Release](https://img.shields.io/github/v/release/ydb-platform/fq-connector-go.svg?style=flat-square)](https://github.com/ydb-platform/fq-connector-go/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/ydb-platform/fq-connector-go)](https://goreportcard.com/report/github.com/ydb-platform/fq-connector-go)
[![Coverage](https://codecov.io/github/ydb-platform/fq-connector-go/graph/badge.svg?token=RCXN9X391Y)](https://codecov.io/github/ydb-platform/fq-connector-go)

### Objectives

Service `fq-connector-go` is a part of YDB Federated Query.
It's an extension point making YDB capable of interaction with various external data sources.
You can deploy `fq-connector-go` alongside with YDB in order to query and handle
data extracted from the external sources.

Currently supported data sources:
* ClickHouse
* PostgreSQL / Greenplum
* YDB
* Microsoft SQL Server
* MySQL / MariaDB
* Oracle
* MongoDB
* Redis

### Documentation 

* [Architecture overview](https://ydb.tech/docs/ru/concepts/federated_query/architecture)
* YDB Federated Query deployment:
    * [For quickstart](https://ydb.tech/docs/ru/getting_started/self_hosted/ydb_docker#zapusk-ydb-federated-query-v-docker) 
    * For production environment: [YDB](https://ydb.tech/docs/ru/deploy/manual/deploy-ydb-federated-query) | [Connector](https://ydb.tech/docs/ru/deploy/manual/connector)
* [Contribution guide](./docs/contribution.md)
* [Type mapping table](./docs/type_mapping_table.md)
