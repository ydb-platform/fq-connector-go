package suite

import (
	"context"
	"fmt"
	"testing"
	"time"

	testify_suite "github.com/stretchr/testify/suite"
	"golang.org/x/exp/constraints"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type Base[T constraints.Integer, K test_utils.ArrowIDBuilder[T]] struct {
	testify_suite.Suite
	*State
	Connector common.TestingServer
	name      string
}

func (b *Base[_, _]) BeforeTest(_, testName string) {
	fmt.Printf("\n>>>>>>>>>> TEST STARTED: %s/%s <<<<<<<<<<\n\n", b.name, testName)
}

func (b *Base[_, _]) TearDownTest() {
	if b.T().Failed() {
		// Do not launch other tests if this test failed
		b.T().FailNow()
	}
}

func (b *Base[_, _]) BeforeSuite(_ string) {
	fmt.Printf("\n>>>>>>>>>> SUITE STARTED: %s <<<<<<<<<<\n", b.name)
}

func (b *Base[_, _]) SetupSuite() {
	// We want to run a distinct instance of Connector for every suite
	var err error

	b.Connector, err = server.NewEmbedded(
		server.WithLoggerConfig(
			&config.TLoggerConfig{
				LogLevel:              config.ELogLevel_DEBUG,
				EnableSqlQueryLogging: true,
			},
		),
		server.WithConversionConfig(
			&config.TConversionConfig{
				UseUnsafeConverters: true,
			},
		),
		server.WithMetricsServerConfig(
			&config.TMetricsServerConfig{
				Endpoint: &api_common.TEndpoint{
					Host: "localhost",
					Port: 8766,
				},
			},
		),
	)
	b.Require().NoError(err)
	b.Connector.Start()
}

func (b *Base[_, _]) TearDownSuite() {
	b.Connector.Stop()

	fmt.Printf("\n>>>>>>>>>> Suite stopped: %s <<<<<<<<<<\n", b.name)
}

type validateTableOptions struct {
	TypeMappingSettings *api_service_protos.TTypeMappingSettings
	Predicate           *api_service_protos.TPredicate
}

func NewDefaultValidateTableOptions() *validateTableOptions {
	return &validateTableOptions{
		TypeMappingSettings: &api_service_protos.TTypeMappingSettings{
			DateTimeFormat: api_service_protos.EDateTimeFormat_YQL_FORMAT,
		},
	}
}

type ValidateTableOption interface {
	Apply(o *validateTableOptions)
}

type withDateTimeFormatOption struct {
	val api_service_protos.EDateTimeFormat
}

func (o withDateTimeFormatOption) Apply(options *validateTableOptions) {
	options.TypeMappingSettings.DateTimeFormat = o.val
}

func WithDateTimeFormat(val api_service_protos.EDateTimeFormat) ValidateTableOption {
	return withDateTimeFormatOption{val: val}
}

type withPredicateOption struct {
	val *api_service_protos.TPredicate
}

func (o withPredicateOption) Apply(options *validateTableOptions) {
	options.Predicate = o.val
}

func WithPredicate(val *api_service_protos.TPredicate) ValidateTableOption {
	return &withPredicateOption{
		val: val,
	}
}

func (b *Base[T, K]) ValidateTable(ds *datasource.DataSource, table *test_utils.Table[T, K], customOptions ...ValidateTableOption) {
	for _, dsi := range ds.Instances {
		b.doValidateTable(table, dsi, customOptions...)
	}
}

func (b *Base[T, K]) doValidateTable(
	table *test_utils.Table[T, K],
	dsi *api_common.TDataSourceInstance,
	customOptions ...ValidateTableOption,
) {
	options := NewDefaultValidateTableOptions()
	for _, option := range customOptions {
		option.Apply(options)
	}

	b.Require().NotEmpty(table.Name)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// describe table
	describeTableResponse, err := b.Connector.ClientBuffering().DescribeTable(ctx, dsi, options.TypeMappingSettings, table.Name)
	b.Require().NoError(err)
	b.Require().Equal(Ydb.StatusIds_SUCCESS, describeTableResponse.Error.Status, describeTableResponse.Error.String())

	// verify schema
	schema := describeTableResponse.Schema
	table.MatchSchema(b.T(), schema)

	// list splits
	slct := &api_service_protos.TSelect{
		DataSourceInstance: dsi,
		What:               common.SchemaToSelectWhatItems(schema, nil),
		From: &api_service_protos.TSelect_TFrom{
			Table: table.Name,
		},
	}

	if options.Predicate != nil {
		slct.Where = &api_service_protos.TSelect_TWhere{
			FilterTyped: options.Predicate,
		}
	}

	listSplitsResponses, err := b.Connector.ClientBuffering().ListSplits(ctx, slct)
	b.Require().NoError(err)
	b.Require().Len(listSplitsResponses, 1)

	// read splits
	splits := common.ListSplitsResponsesToSplits(listSplitsResponses)
	readSplitsResponses, err := b.Connector.ClientBuffering().ReadSplits(ctx, splits)
	b.Require().NoError(err)
	b.Require().Len(readSplitsResponses, 1)

	records, err := common.ReadResponsesToArrowRecords(readSplitsResponses)
	b.Require().NoError(err)

	// verify data
	table.MatchRecords(b.T(), records, schema)
}

func NewBase[T constraints.Integer, K test_utils.ArrowIDBuilder[T]](t *testing.T, state *State, name string) *Base[T, K] {
	b := &Base[T, K]{
		State: state,
		name:  name,
	}

	b.SetT(t)

	return b
}
