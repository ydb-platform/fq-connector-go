module github.com/ydb-platform/fq-connector-go

go 1.23

require (
	github.com/ClickHouse/ch-go v0.58.2
	github.com/ClickHouse/clickhouse-go/v2 v2.18.0
	github.com/OneOfOne/xxhash v1.2.8
	github.com/apache/arrow/go/v13 v13.0.0-20230512153032-cd6e2a4d2b93
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/denisenkom/go-mssqldb v0.12.2
	github.com/dustin/go-humanize v1.0.1
	github.com/go-mysql-org/go-mysql v1.6.0
	github.com/golang/protobuf v1.5.4
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.5.3
	github.com/hashicorp/go-retryablehttp v0.7.4
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa
	github.com/jackc/pgx/v5 v5.5.5
	github.com/mattn/go-sqlite3 v1.14.24
	github.com/opensearch-project/opensearch-go/v4 v4.1.0
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.16.0
	github.com/prometheus/client_model v0.5.0
	github.com/prometheus/common v0.44.0
	github.com/prometheus/procfs v0.11.1
	github.com/redis/go-redis/v9 v9.7.0
	github.com/shirou/gopsutil/v3 v3.24.2
	github.com/sijms/go-ora/v2 v2.8.19
	github.com/spf13/cobra v1.8.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.9.0
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20241112172322-ea1f63298f77
	github.com/ydb-platform/ydb-go-sdk/v3 v3.108.0
	github.com/ydb-platform/ydb-go-yc v0.11.0
	go.mongodb.org/mongo-driver v1.17.1
	go.uber.org/atomic v1.11.0
	go.uber.org/zap v1.26.0
	golang.org/x/exp v0.0.0-20240222234643-814bf88cf225
	golang.org/x/sync v0.10.0
	golang.org/x/time v0.5.0
	golang.org/x/xerrors v0.0.0-20231012003039-104605ab7028
	gonum.org/v1/gonum v0.14.0
	google.golang.org/grpc v1.65.0
	google.golang.org/protobuf v1.34.1
	gopkg.in/yaml.v3 v3.0.1
	sigs.k8s.io/yaml v1.3.0
)

require (
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/apache/thrift v0.16.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/frankban/quicktest v1.14.5 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.6.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/goccy/go-json v0.10.0 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.0 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v23.1.21+incompatible // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/klauspost/cpuid/v2 v2.2.3 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/paulmach/orb v0.11.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 // indirect
	github.com/siddontang/go-log v0.0.0-20190221022429-1e957dd83bed // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/yandex-cloud/go-genproto v0.0.0-20240425114406-68c9b49389a1 // indirect
	github.com/ydb-platform/ydb-go-yc-metadata v0.5.3 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/otel v1.22.0 // indirect
	go.opentelemetry.io/otel/trace v1.22.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/net v0.33.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240528184218-531527333157 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
