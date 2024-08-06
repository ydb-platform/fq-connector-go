# Инструкция по разработке коннектора

## Концепция

**Коннектор** - специальный микросервис, выступающий в роли прокси между YDB и внешними источниками данных. Коннекторы формируют специальный слой абстракции, изолирующий YDB от специфики сторонних хранилищ. Благодаря этому YDB может через один и тот же интерфейс работать с разнообразными источниками данных.

Заметим, что внутри YDB активно применяется и более низкоуровневая абстракция аналогичного назначения - **[провайдеры](https://github.com/ydb-platform/ydb/tree/main/ydb/library/yql/providers/)**. Это библиотеки, написанные на языке С++ и отвечающие за оптимизацию запросов и выполнение ввода-вывода во внешние источники данных. Большинство провайдеров поддерживают только какой-то один источник данных (например, провайдер `S3` отвечает только за работу с объектным хранилищем); их разработка чрезвычайно трудозатратна.

В связи с этим было принято решение реализовать `Generic` провайдер - универсальную библиотеку, через которую YDB сможет работать с любыми источниками данных посредством обращений к внешниму микросервису - коннектору. 

![Providers vs connector](./providers_vs_connector.png)

Благодаря этому архитектурному решению добавление новых источников значительно облегчается, а кодовая база YDB не распухает от новых зависимостей и остаётся относительно стабильной. Коннектор может быть реализован на любом языке программирования по имеющейся GRPC-спецификации.

### Сетевой интерфейс коннектора

Коннектор `fq-connector-go` - типичный микросервис, который может одновременно отвечать на запросы сразу по нескольким слушающим сокетам:
* Основной GRPC-сервер - порт `2130` ([Protobuf API](https://github.com/ydb-platform/ydb/tree/main/ydb/library/yql/providers/generic/connector/api) хранится в репозитории YDB).
* HTTP-сервер, отдающий статистику - порт `8766`.
* HTTP-сервер профилировщика Go Runtime - порт `6060`.

В production среде в качестве клиента к коннектору выступает сам YDB (исполняемый файл `ydbd`).

В процессе разработки и отладки для обращения к коннектору можно пользоваться:
* Встроенной командой `fq-connector-go client`;
* Встроенной командой `fq-connector-go bench`;
* Инструментом [dqrun](https://github.com/ydb-platform/ydb/tree/24.1.9/ydb/library/yql/tools/dqrun), основанным на кодовой базе YDB;
* Инструментом [kqprun](https://github.com/ydb-platform/ydb/tree/24.1.9/ydb/tests/tools/kqprun), основанным на кодовой базе YDB.
* Непосредственно через Web UI YDB.

### Протокол работы коннектора

Любой пользовательский запрос в YDB (да и во всех современных базах данных) выполняется в два этапа:
* **Фаза оптимизации** запроса. В оперативной памяти YDB запрос представляется виде графа, узлами которого являются "лямбды" - функции, описывающие процесс извлечения, обработки и преобразования данных. Специальные оптимизаторы многократно обходят этот граф и трансформируют его с целью ускорения фазы выполнения. В конечном итоге из графа конструируется внутренняя "программа", которая исполняется движком на следующем этапе.
* **Фаза выполнения** запроса или "runtime". На данном этапе движок потоково извлекает данные из внешних источников и выполняет над этими данными операции в соответствии с запросом пользователя.

![Sequence diagram](./sequence_diagram.png)

В фазе оптимизации запроса YDB обращается за метаданными таблицы через метод `DescribeTable`. В фазе выполнения запроса YDB сначала просит коннектор разбить таблицу на **сплиты** (split - в большинстве случаев это синоним горизонтальной партиции таблицы) с помощью метода `ListSplits`, а затем извлекает данные сплитов через метод `ReadSplits`. 

### DataSource

Внешние источники данных в сервисе `fq-connector-go` скрываются за интерфейсом [DataSource](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.6/app/server/datasource#DataSource). У него всего-навсего два метода: метод для описания метаданных таблицы и для извлечения данных. Несмотря на лаконичность интерфейса, имплементировать его придётся постепенно, по частям, добавляя новую функциональность. 

Логика работы с реляционными СУБД может быть в значительной степени обобщена и переиспользована в коде, относящемся к разным базам данных, поэтому имплементация `DataSource` для РСУБД у нас на данный момент [одна](https://github.com/ydb-platform/fq-connector-go/blob/v0.2.6/app/server/datasource/rdbms/data_source.go#L26) - с помощью структуры [Preset](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.6/app/server/datasource/rdbms#Preset) в ней меняются только источнико-специфичные части:

* [ConnectionManager](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.6/app/server/datasource/rdbms/utils#ConnectionManager) отвечает за создание сетевых соединений, которые описываются абстракцией [Connection](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.6/app/server/datasource/rdbms/utils#Connection). Этот интерфейс напоминает усечённую версию `*sql.DB` из стандартной библиотеки.
* [SQLFormatter](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.6/app/server/datasource/rdbms/utils#ConnectionManager) формирует запросы к источнику на принятом у него диалекте SQL.
* [TypeMapper](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.6/app/server/datasource#TypeMapper) отвечает за преобразование метаданных о таблице из системы типов источника данных в систему типов языка `YQL`, использующегося в `YDB`, то есть отвечает за одно из преобразований типов, подробно описанных ниже.
* [SchemaProvider](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.6/app/server/datasource/rdbms/utils#SchemaProvider) извлекает метаинформацию о таблице (количество, имена и типы столбцов), чтобы в дальнейшем отправить её в `YDB` в понятном ей формате.

Эти интерфейсы - наиболее верхнеуровневые, но есть ещё и несколько вспомогательных. В написании имлпементаций этих интерфейсов и заключается наша основная задача.

### О трансформации данных и метаданных

Коннектор должен превращать (трансформировать) данные из внешних систем в формат, поддерживаемый YDB, а также описывать эти данные в понятной YDB системе типов. 

С точки зрения работы с **метаданными** такой системой типов является [система типов](https://ydb.tech/docs/ru/yql/reference/types/) языка YQL. Описания типов хранятся в [Public Protobuf API YDB](https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/protos/ydb_value.proto). По запросу от YDB (метод `DescribeTable`) коннектор должен извлечь описание таблицы из источника (это описание, разумеется, хранится в системе типов, специфичной для источника) и предоставить схему таблицы в системе типов YQL.

В качестве формата передачи **данных** используется колоночный формат [Apache Arrow (тип IPC Streaming)](https://arrow.apache.org/docs/cpp/api/ipc.html). Колоночное представление данных часто встречается в аналитических СУБД, поскольку позволяет сэкономить дорогостоящие операции ввода-вывода. В Arrow используется собственная [система типов](https://arrow.apache.org/docs/cpp/api/datatype.html). При этом коннекторы вычитывают данные из соединения с внешним источником данных в объекты-приёмники ([Acceptor](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.6/app/server/paging#Acceptor)), которые описываются в системе типов Go: `rows.Scan(acceptors...)`. Уже позднее эти объекты накапливаются в колоночных буферах ([ColumnarBuffer](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.5/app/server/paging#ColumnarBuffer)), те, в свою очередь, сериализуются и отправляются по сети в сторону YDB в формате Arrow.

Таким образом в коннекторе встречаются сразу 4 системы типов:
* Система типов YDB (YQL).
* Система типов источника данных.
* Система типов Apache Arrow.
* Система типов языка Go.

![Type Mapping](./type_mapping.png)

Код, выполняющий *трансформацию* между этими системами типов, традиционно сконцентрирован в файлах `type_mapper.go` ([PG](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/datasource/rdbms/postgresql/type_mapper.go), [CH](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/datasource/rdbms/clickhouse/type_mapper.go)). 

Ещё один смысл, вкладываемый в термин *трансформации* данных - это  преобразование данных из строкового в колоночное представление. Логика перекладывания данных из элементов строки (row) в колоночные буфера реализована однократно для всех источников данных в функции [RowTransformerDefault.AppendToArrowBuilders](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.6/app/server/paging#RowTransformerDefault.AppendToArrowBuilders).

![Data transformation](./append_to_arrow_builders.png)


## Пошаговая инструкция

### Первые шаги

Работу по добавлению нового источника можно начать с создания в папке [rdbms](https://github.com/ydb-platform/fq-connector-go/tree/main/app/server/datasource/rdbms) подпапки для нового источника данных. Нейминг должен соответствовать [enum](https://github.com/ydb-platform/ydb/blob/main/ydb/library/yql/providers/generic/connector/api/common/data_source.proto#L29-L37) из YDB API. В этой папке можно реализовать перечисленные выше интерфейсы в самом примитивном виде (на заглушках), и заполнить ими структуру `Preset`.

Сразу после этого новый источник данных надо подключить [в фабрике](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/datasource/rdbms/data_source_factory.go#L27-L40) источников. После этого вы сможете делать обращения к коннектору через тестовый клиент `fq-connector-go client`.

Скомпилируйте и запустите коннектор командой:
```
make build
make run
```

Затем подготовьте файл с конфигурацией клиента [по примеру](https://github.com/ydb-platform/fq-connector-go/blob/main/scripts/debug/config/client/pg.local.txt) и попробуйте сходить в коннектор:

```
./fq-connector-go client connector --config ./your/config.txt --table some_table_name
```

Если в коде сервиса не будет ошибок, вы получите какие-то ответы (в соответствии с данными, "зашитыми" в заглушках). После этого можно приступать к наполнению `DataSource` источнико-специфичным кодом.

### ConnectionManager

Начать стоит с реализации интерфейса `СonnectionManager`. Здесь вам нужно просто научиться по параметрам, пришедшим в структуре типа `TDataSourceInstance`, конструировать сетевое соединение к базе. Наиболее хрестоматийные примеры можно посмотреть в папках [clickhouse](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/datasource/rdbms/clickhouse/connection_manager.go) и [postgresql](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/datasource/rdbms/postgresql/connection_manager.go).

> [!IMPORTANT]
> Для работы с внешними источниками данных вам потребуется **драйвер** - библиотека на языке Go, которая реализует протокол взаимодействия с базой. Существуют важные нюансы при выборе библиотек:
> * Лицензионная чистота (используем только MIT, Apache, BSD и подобные permissive лицензии; из лицензий с ограничениями разрешена MPL-2.0).
> * При прочих равных стараемся выбирать библиотеку, которая не встраивается в `database/sql`, а предоставляет свою реализацию всех необходимых нам абстракций (стандартная библиотека Go в этом месте тормозит, так как использует `reflect`).
> * Существует закрытый для внешних лиц перечень разрешённых версий сторонних библиотек. Когда выберете библиотеку, уточните у ментора, какую версию данной библиотеки можно использовать.

Некоторые источники данных предоставляют несколько сетевых интерфейсов для доступа данных: например, к ClickHouse можно подключиться как по TCP-протоколу, так и по HTTP-протоколу. Изучите ваш источник данных в этом отношении. В большинстве случаев достаточно только реализации `NATIVE` (то есть TCP) протокола.

Иногда при соединении с источником требуется указать какие-то особенные параметры, например, у PostgreSQL есть понятие схемы (пространства имён для таблиц). Если вам недостаточн общее параметров, уже присутствующих в структуре [TDataSourceInstance](https://github.com/ydb-platform/ydb/blob/main/ydb/library/yql/providers/generic/connector/api/common/data_source.proto#L65-L86), вы можете добавить в опциональное поле `options` новую структуру, описывающую специфику именно вашего источника.

### Connection, Rows и трансформеры

`ConnectionManager` должен возвращать абстракцию соединения - [Connection](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.6/app/server/datasource/rdbms/utils#Connection). Соединение умеет выполнять запросы (метод `Query`). Результатом обработки запроса является интерфейс [Rows](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.5/app/server/datasource/rdbms/utils#Rows). Фактически это итератор, сильно напоминающий по интерфейсу `sql.Rows`. С помощью него имплементация `DataSource` может вычитывать данные из соединения с РСУБД потоково, строчка за строчкой.

У `Rows` есть важный метод - `MakeTransformer`, который возвращает шаблонный интерфейс `RowsTransformer[Acceptor]`. Он выполняет большую часть работы по конвертации данных между разными системами типов. В остальном работа с `Rows` практически не отличается от работы с `sql.Rows` из стандартной библиотеки.

### Поддержка нового источника данных в YDB

Итак, вы успешно смогли прочитать данные с помощью отладочного клиента к `fq-connector-go`. Финальный этап работ - сделать так, чтобы к вашему источнику данных мог обратиться самый важный клиент к коннектору - само YDB. Для этого необходимо внести изменения в его кодовую базу.

> [!IMPORTANT]
> Компиляция YDB из исходников требует больших вычислительных мощностей и может занимать очень много времени (на сервере с 56 ядрами - около 3 часов). Здесь на помощь приходит кэш артефактов компиляции, который поддерживается мейнтейнерами YDB. Этот кэш прогревается ежедневно во время ночных сборок ветки `main`. Поэтому если вы хотите, чтобы при локальных сборках с помощью `ya` hit rate кэша оставался достаточно высоким, вам необходимо поддерживать свои исходники в относительно актуальном состоянии и периодически ребейзиться на `main` апстрима. Достичь этого можно, например, так:
> ```
> gh repo sync юзернейм/ydb -s ydb-platform/ydb
> git checkout main
> git pull origin main
> git checkout feature-branch
> git rebase origin/main
> ```

Для поддержки нового источника в YDB предлагается следующий алгоритм:

1. Форкните [репозиторий](https://github.com/ydb-platform/ydb) YDB.
Склонируйте репозиторий на ту машину, где у вас будет идти разработка YDB. Эта машина должна быть достаточно мощной (минимум 16 ядер CPU , 32 Gb RAM), и создайте рабочую ветку.
    ```
    git clone git@gitlab.com:юзернейм/ydb.git  
    cd ydb 
    git checkout -b feature-branch
    ```
1. Выполните команду.
    ```
    ./ya ide vscode-clangd -P ~/projects/ydb.vscode-clangd ydb contrib/libs
    ```
1. В целевой папке появится workspase для VSCode.
1. (если работаете на виртуальной машине) В VSCode надо поставить [плагин](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-ssh) для удалённой работы по ssh и зайти на хост.
1. В VSCode на целевой машине надо поставить [плагин](https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd) с поддержкой clangd.
1. После открытия воркспейса clangd начнёт индексацию проекта (ориентируйтесь на несколько часов).
1. Проверьте ваш git global user.name и user.email командой:
    ```
    git config --list --show-origin 
    ```
    Если там не указано ничего, или указаны не ваши данные, поменяйте их командыми:
   
    ```
    git config --global user.name "ваш user.name"
    ```
    
    ```
    git config --global user.email "ваш user.email"
    ```
    Это важно для того, чтобы в вашем профиле Гитхаб отображались PRы в Ydb.
1. Скомпилируйте инструмент `kqprun` с помощью встроенного инструмента `ya`:
    ```
    ./ya make --build relwithdebinfo ydb/tests/tools/kqprun
    ```
1. Разверните свой источник данных в виде Docker-контейнера.
1. Создайте какую-нибудь таблицу в вашем источнике данных (хороший GUI-инструмент для реляционных баз данных - [DBeaver](https://dbeaver.io/)).
1. Разверните сервис коннектора (например, `make run`).
1. Подготовьте файл `app_conf.txt`, в котором укажите хост и порт для подключения к сервису коннектора:
    ```prototext
    FeatureFlags {
        EnableExternalDataSources: true
        EnableScriptExecutionOperations: true
    }

    QueryServiceConfig {
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
1. Подготовьте YQL-скрипт `schema.yql`, который регистрирует ваш источник данных как внешний для YDB, а также укажите пароль для доступа к источнику. Подставьте актуальные значения во все поля.
    ```sql
    CREATE OBJECT secret_password (TYPE SECRET) WITH (value = "<password>");

    CREATE EXTERNAL DATA SOURCE external_data_source WITH (
        SOURCE_TYPE="<data_source_type>",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<table>",
        AUTH_METHOD="BASIC",
        LOGIN="<username>",
        PASSWORD_SECRET_NAME="secret_password",
        PROTOCOL="NATIVE",
        USE_TLS="FALSE"
    );
    ```
1. Подготовьте YQL-скрипт для извлечения данных `data.yql`, где вместо `<table_name>` подставьте имя таблицы, которую создали на одном из предыдущих шагов.
    ```sql
    SELECT * FROM external_data_source.<table_name>
    ```
1. Вызовите `./kqprun` следующей командой
    ```sh
    ./kqprun -s schema.yql -p data.yql --app-config=app_conf.txt
    ```
    Если в результате вызова вы увидели JSON, похожий на те данные, что вы положили в таблицу, поздравляю - ваша работа окончена. Но с первого раза, конечно, ничего не получится. Проанализируйте ошибку, исправьте код и продолжайте компилировать и запускать `kqprun` до тех пор, пока не почините все ошибки.

Можно выделить несколько областей кода в YDB, которые нуждаются в добавлении нового источника данных:
* [YQL Providers](https://github.com/ydb-platform/ydb/blob/24.1.14/ydb/library/yql/providers/generic/provider):
    * https://github.com/ydb-platform/ydb/blob/main/ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h#L11-L44
    * https://github.com/ydb-platform/ydb/blob/24.1.14/ydb/library/yql/providers/generic/provider/yql_generic_load_meta.cpp#L267-L293
    * https://github.com/ydb-platform/ydb/blob/24.1.14/ydb/library/yql/providers/generic/provider/yql_generic_load_meta.cpp#L319-L331
    * https://github.com/ydb-platform/ydb/blob/24.1.14/ydb/library/yql/providers/generic/provider/yql_generic_dq_integration.cpp#L191-L207
    * https://github.com/ydb-platform/ydb/blob/6f2b38f212e36e0bcd0729525aef2e04494141a0/ydb/library/yql/providers/generic/actors/yql_generic_provider_factories.cpp#L34-L37
    * https://github.com/ydb-platform/ydb/blob/24.1.14/ydb/library/yql/providers/generic/provider/yql_generic_dq_integration.cpp#L158-L171
    * https://github.com/ydb-platform/ydb/blob/38a7ef26dd27509de68226e2d1117ed6ef933646/ydb/library/yql/providers/generic/provider/yql_generic_dq_integration.cpp#L24-L41
    * https://github.com/ydb-platform/ydb/blob/e5ae52da8cfbfcdfa05ff85a236b85f19419d168/ydb/library/yql/providers/generic/provider/yql_generic_cluster_config.cpp#L195
* [External Sources](https://github.com/ydb-platform/ydb/blob/24.1.14/ydb/core/external_sources/):
    * https://github.com/ydb-platform/ydb/blob/24.1.14/ydb/core/external_sources/external_source_factory.cpp#L35-L55
    * https://github.com/ydb-platform/ydb/blob/a6eb07c046fa5e88777549a74558adc62787aaf0/ydb/core/external_sources/external_data_source.cpp#L39-L41
* [Proto](https://github.com/ydb-platform/ydb/blob/main/ydb/library/yql/providers/generic/connector/api/common/data_source.proto)
    * https://github.com/ydb-platform/ydb/blob/main/ydb/library/yql/providers/generic/connector/api/common/data_source.proto#L29-L38
* [DDL](https://github.com/ydb-platform/ydb/blob/2f30f742e93b5da271129a91b5d2093ef52da21b/ydb/core/kqp/gateway/behaviour/external_data_source/manager.cpp) (Если потребуется)
    * https://github.com/ydb-platform/ydb/blob/2f30f742e93b5da271129a91b5d2093ef52da21b/ydb/core/kqp/gateway/behaviour/external_data_source/manager.cpp#L72-L79


Список этих файлов может быть неисчерпывающим; если заметите что-то ещё - PRs are welcome :) 

Примеры PR в YDB
* Oracle
    * https://github.com/ydb-platform/ydb/pull/6723/files

Примеры PR в fq-connector-go
* MS SQL Server
    * https://github.com/ydb-platform/fq-connector-go/pull/93
* MySQL
    * https://github.com/ydb-platform/fq-connector-go/pull/94


## Изменения в API и конфигурации коннектора

Периодически возникает необходимость как-либо расширить API Коннектора (например, добавить туда [что-то специфичное](https://github.com/ydb-platform/ydb/blob/24.1.9/ydb/library/yql/providers/generic/connector/api/common/data_source.proto#L79-L83) для вашего источника данных) или поменять его [конфигурацию](https://github.com/ydb-platform/fq-connector-go/tree/main/app/config). И API, и конфигурация описываются в виде Protobuf-файлов, по которым генерируется исходный код на языке Go. Сгенерированные файлы сохраняются в репозитории в `fq-connector-go`.

Чтобы регенерировать исходники, выполните следующую команду:

```bash
# клонируйте репозиторий YDB
git clone git@github.com:ydb-platform/ydb.git

# при необходимости внесите изменения в исходники YDB

# перейдите в папку с исходинками коннектора и запустите скрипт
cd path/to/fq-connector-go/repo
./generate.py --ydb-repo=path/to/ydb/repo --connector-repo=path/to/fq-connector-go/repo

# Если вы вносили изменения в исходники YDB, не забудьте закоммитить их в апстрим через процедуру code review.
```
