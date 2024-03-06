# Добавление нового источника данных

## Концепция

### DataSource

Внешние источники данных в сервисе `fq-connector-go` скрываются за интерфейсом [DataSource](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.5/app/server/datasource#DataSource). У него всего-навсего два метода: метод для описания метаданных таблицы и для извлечения данных. Несмотря на лаконичность интерфейса, имплементировать его придётся постепенно, по частям, добавляя новую функциональность. 

Логика работы с реляционными СУБД может быть в значительной степени обобщена и переиспользована в коде, относящемся к разным источникам. Поэтому имплементация `DataSource` для РСУБД у нас на данный момент [одна](https://github.com/ydb-platform/fq-connector-go/blob/v0.2.5/app/server/datasource/rdbms/data_source.go#L26) - в ней меняются только источнико-специфичные части, которые параметризуют поведение `rdbms.dataSourceImpl` с помощью структуры [Preset](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.5/app/server/datasource/rdbms#Preset):

* `ConnectionManager` отвечает за работу с сетевыми соединениями.
* `SQLFormatter` рендерит запросы к источнику на принятом у него диалекте SQL.
* `TypeMapper` отвечает за преобразование типов данных из системы типов источника данных в систему типов `YDB`.
* `SchemaProvider` извлекает метаинформацию о таблице (количество, имена и типы столбцов), чтобы в дальнейшем отправить её в `YDB` в понятном ей формате.

Эти 4 интерфейса - наиболее верхнеуровневые, но есть ещё и несколько вспомогательных. В реализации этих четырёх классов и заключается наша основная задача.

### О трансформации данных и метаданных

Основное назначение коннектора - выступать в роли слоя абстракции между YDB и внешним источником данных. Он должен превращать (трансформировать) данные из внешних систем в формат, поддерживаемый YDB, а также описывать эти данные в понятной YDB системе типов. 

С точки зрения работы с метаданными такой системой типов является [система типов](https://ydb.tech/docs/ru/yql/reference/types/) языка YQL. Описания типов хранятся в [Public Protobuf API YDB](https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/protos/ydb_value.proto). Это означает, что по запросу от YDB (метод `DescribeTable`) коннектор должен извлечь из источника данных схему нужной таблицы в системе типов источника и предоставить её в системе типов YQL.

В качестве формата передачи данных используется колоночный формат [Apache Arrow (тип IPC Streaming)](https://arrow.apache.org/docs/cpp/api/ipc.html). Колоночное представление данных часто встречается в аналитических СУБД, поскольку позволяет экономить IO. В Arrow используется собственная [система типов](https://arrow.apache.org/docs/cpp/api/datatype.html). При этом мы вычитываем данные из соединения с внешним источником данных в объекты-приёмники, которые описываются в системе типов Go: `rows.Scan(acceptors...)`. Уже позднее эти объекты накапливаются в колоночных буферах [ColumnarBuffer](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.5/app/server/paging#ColumnarBuffer), те, в свою очередь, сериализуются и отправляются по сети в сторону YDB в формате Arrow.

Таким образом в коннекторе встречаются сразу 4 системы типов:
* Система типов YDB (YQL).
* Система типов источника данных.
* Система типов Apache Arrow.
* Система типов языка Go.

![Type Mapping](./type_mapping.png)

Код, выполняющий *трансформацию* между этими системами типов, традиционно сконцентрирован в файлах `type_mapper.go` ([PG](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/datasource/rdbms/postgresql/type_mapper.go), [CH](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/datasource/rdbms/clickhouse/type_mapper.go)). 

Ещё один смысл, вкладываемый в термин *трансформации* данных - это  преобразование данных из строкового в колоночное представление. Логика перекладывания данных из элементов строки (row) в колоночные буфера реализована однократно для всех источников данных в функции [RowTransformerDefault.AppendToArrowBuilders](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.5/app/server/paging#RowTransformerDefault.AppendToArrowBuilders).

![Data transformation](./append_to_arrow_builders.png)

### Сетевой интерфейс коннектора

Коннектор - типичный микросервис, который может одновременно отвечать на запросы сразу по нескольким слушающим сокетам:
* Основной GRPC-сервер - порт `2130` ([Protobuf API](https://github.com/ydb-platform/ydb/tree/main/ydb/library/yql/providers/generic/connector/api) хранится в репозитории YDB).
* HTTP-сервер, отдающий статистику - порт `8766`.
* HTTP-сервер профилировщика Go Runtime - порт `6060`.

Если вы что-то поменяли в публичном API Коннектора, склонируйте на локальную машину репозиторий с YDB и регенерируйте исходники следующей командой:

```
./generate.py --ydb-repo=path/to/ydb/repo --connector-repo=path/to/fq-connector-go/repo
```

## Пошаговая инструкция

### Первые шаги

Начать работу надо с того, чтобы создать в папке [rdbms](https://github.com/ydb-platform/fq-connector-go/tree/main/app/server/datasource/rdbms) подпапку, соответствующую вашему источнику данных. Нейминг должен соответствовать [enum](https://github.com/ydb-platform/ydb/blob/main/ydb/library/yql/providers/generic/connector/api/common/data_source.proto#L29-L37) из YDB API. Там можно реализовать 4 приведенных выше интерфейса в самом примитивном виде, то есть на заглушках, и заполнить ими структуру `Preset`.

Сразу после этого новый источник данных надо подключить [в фабрике](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/datasource/rdbms/data_source_factory.go#L27-L40) источников. После этого вы сможете делать обращения к коннектору через тестовый клиент `fq-connector-go client`.

Скомпилируйте и запустите коннектор командой:
```
make build
make run
```

Затем подготовьте файл с конфигурацией клиента [по примеру](https://github.com/ydb-platform/fq-connector-go/blob/main/scripts/debug/config/client/pg.local.txt) и попробуйте сходить в коннектор:

```
./fq-connector-go client ./your/config.txt
```

Если всё прошло хорошо, вы получите какие-то ответы (в соответствии со сделанными вами заглушками), либо сервис упадёт с паникой, и вы пойдёте её чинить.

Затем приступите к наполнению `DataSource` источнико-специфичным кодом.

### ConnectionManager

Начать стоит с реализации интерфейса `СonnectionManager`. Здесь вам нужно просто научиться по параметрам, пришедшим в структуре типа `TDataSourceInstance`, конструировать сетевое соединение к базе. Наиболее хрестоматийные примеры можно посмотреть в папках [clickhouse](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/datasource/rdbms/clickhouse/connection_manager.go) и [postgresql](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/datasource/rdbms/postgresql/connection_manager.go).

> [!IMPORTANT]
> Для работы с внешними источниками данных вам потребуется драйвер - библиотека на языке Go, которая реализует протокол взаимодействия с базой. Существуют важные нюансы при выборе библиотек:
> * Лицензионная чистота (используем только MIT, Apache, BSD и подобные permissive лицензии).
> * При прочих равных стараемся выбирать библиотеку, которая не встраивается в `database/sql`, а предоставляет свою реализацию всех необходимых нам абстракций (стандартная библиотека Go в этом месте тормозит, так как использует `reflect`).
> * Существует закрытый для внешних лиц перечень разрешённых версий сторонних библиотек. Когда выберете библиотеку, уточните у ментора, какую версию можно использовать.

Некоторые источники данных предоставляют несколько сетевых интерфейсов для доступа данных: например, к ClickHouse можно подключиться как по TCP-протоколу, так и по HTTP-протоколу. Изучите ваш источник данных в этом отношении. В большинстве случаев достаточно только реализации `NATIVE` (то есть TCP-протокола).

Иногда при соединении с источником требуется указать какие-то особенные параметры, например, у PostgreSQL есть понятие схемы (что-то вроде пространства имён для таблиц). Если вам недостаточно параметров, уже присутствующих в структуре [TDataSourceInstance](https://github.com/ydb-platform/ydb/blob/main/ydb/library/yql/providers/generic/connector/api/common/data_source.proto#L65-L86), вы можете добавить в опциональное поле `options` новую структуру, описывающую специфику вашего источника.

### Connection и Rows

`ConnectionManager` должен возвращать абстракцию соединения - [Connection](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.5/app/server/datasource/rdbms/utils#Connection). Соединение умеет выполнять запросы (метод `Query`). Результатом обработки запроса является интерфейс [Rows](https://pkg.go.dev/github.com/ydb-platform/fq-connector-go@v0.2.5/app/server/datasource/rdbms/utils#Rows). Фактически это итератор, сильно напоминающий по интерфейсу `sql.Rows`. С помощью него мы можем вычитывать данные из соединения с РСУБД потоково, строчка за строчкой.

У `Rows` есть важный метод - `MakeTransformer`, который возвращает шаблонный интерфейс `RowsTransformer[Acceptor]`. Он выполняет большую часть работы по конвертации данных между разными системами типов. В остальном работа с `Rows` практически не отличается от работы с `sql.Rows` из стандартной библиотеки.
