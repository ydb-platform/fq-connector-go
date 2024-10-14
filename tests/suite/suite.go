package suite

import (
	"context"
	"fmt"
	"testing"
	"time"

	testify_suite "github.com/stretchr/testify/suite"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type Base[ID test_utils.TableIDTypes, IDBUILDER test_utils.ArrowIDBuilder[ID]] struct {
	testify_suite.Suite
	*State
	Connector common.TestingServer
	cfg       *baseConfig
}

type baseConfig struct {
	name            string                  // suite name
	embeddedOptions []server.EmbeddedOption // additional launching options for Connector service
}

func (b *Base[_, _]) BeforeTest(_, testName string) {
	fmt.Printf("\n>>>>>>>>>> TEST STARTED: %s/%s <<<<<<<<<<\n\n", b.cfg.name, testName)
}

func (b *Base[_, _]) TearDownTest() {
	if b.T().Failed() {
		// Do not launch other tests if this test failed
		b.T().FailNow()
	}
}

func (b *Base[_, _]) BeforeSuite(_ string) {
	fmt.Printf("\n>>>>>>>>>> SUITE STARTED: %s <<<<<<<<<<\n", b.cfg.name)
}

func (b *Base[_, _]) SetupSuite() {
	// We want to run a distinct instance of Connector for every suite
	var err error

	options := []server.EmbeddedOption{
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
		server.WithConnectionTimeouts("2s", "1s"),
	}

	options = append(options, b.cfg.embeddedOptions...)

	b.Connector, err = server.NewEmbedded(
		options...,
	)
	b.Require().NoError(err)
	b.Connector.Start()
}

func (b *Base[_, _]) TearDownSuite() {
	b.Connector.Stop()

	fmt.Printf("\n>>>>>>>>>> Suite stopped: %s <<<<<<<<<<\n", b.cfg.name)
}

type validateTableOptions struct {
	typeMappingSettings *api_service_protos.TTypeMappingSettings
	predicate           *api_service_protos.TPredicate
	filtering           api_service_protos.TReadSplitsRequest_EFiltering
}

func newDefaultValidateTableOptions() *validateTableOptions {
	return &validateTableOptions{
		typeMappingSettings: &api_service_protos.TTypeMappingSettings{
			DateTimeFormat: api_service_protos.EDateTimeFormat_YQL_FORMAT,
		},
	}
}

type ValidateTableOption interface {
	apply(o *validateTableOptions)
}

type withDateTimeFormatOption struct {
	val api_service_protos.EDateTimeFormat
}

func (o withDateTimeFormatOption) apply(options *validateTableOptions) {
	options.typeMappingSettings.DateTimeFormat = o.val
}

func WithDateTimeFormat(val api_service_protos.EDateTimeFormat) ValidateTableOption {
	return withDateTimeFormatOption{val: val}
}

type withFilteringOption struct {
	val api_service_protos.TReadSplitsRequest_EFiltering
}

func (o withFilteringOption) apply(options *validateTableOptions) {
	options.filtering = o.val
}

func WithFiltering(val api_service_protos.TReadSplitsRequest_EFiltering) ValidateTableOption {
	return withFilteringOption{val: val}
}

type withPredicateOption struct {
	val *api_service_protos.TPredicate
}

func (o withPredicateOption) apply(options *validateTableOptions) {
	options.predicate = o.val
}

func WithPredicate(val *api_service_protos.TPredicate) ValidateTableOption {
	return &withPredicateOption{
		val: val,
	}
}

func (b *Base[ID, IDBUILDER]) ValidateTable(
	ds *datasource.DataSource,
	table *test_utils.Table[ID, IDBUILDER],
	customOptions ...ValidateTableOption,
) {
	for _, dsi := range ds.Instances {
		b.doValidateTable(table, dsi, customOptions...)
	}
}

func (b *Base[ID, IDBUILDER]) doValidateTable(
	table *test_utils.Table[ID, IDBUILDER],
	dsi *api_common.TDataSourceInstance,
	customOptions ...ValidateTableOption,
) {
	options := newDefaultValidateTableOptions()
	for _, option := range customOptions {
		option.apply(options)
	}

	b.Require().NotEmpty(table.Name)

	ctx, cancel := context.WithTimeout(test_utils.NewContextWithTestName(), 60*time.Second)
	defer cancel()

	// describe table
	describeTableResponse, err := b.Connector.ClientBuffering().DescribeTable(ctx, dsi, options.typeMappingSettings, table.Name)
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

	if options.predicate != nil {
		slct.Where = &api_service_protos.TSelect_TWhere{
			FilterTyped: options.predicate,
		}
	}

	listSplitsResponses, err := b.Connector.ClientBuffering().ListSplits(ctx, slct)
	b.Require().NoError(err)
	b.Require().Len(listSplitsResponses, 1)

	// read splits
	splits := common.ListSplitsResponsesToSplits(listSplitsResponses)
	readSplitsResponses, err := b.Connector.ClientBuffering().ReadSplits(ctx, splits)
	b.Require().NoError(err)
	b.Require().NoError(common.ExtractErrorFromReadResponses(readSplitsResponses))
	// either no blocks (empty table), either single block (tables are small)
	b.Require().Contains([]int{0, 1}, len(readSplitsResponses))

	records, err := common.ReadResponsesToArrowRecords(readSplitsResponses)
	b.Require().NoError(err)

	// verify data
	table.MatchRecords(b.T(), records, schema)
}

type BaseOption interface {
	apply(cfg *baseConfig)
}

type embeddedOption struct {
	options []server.EmbeddedOption
}

func (o *embeddedOption) apply(cfg *baseConfig) {
	cfg.embeddedOptions = append(cfg.embeddedOptions, o.options...)
}

func WithEmbeddedOptions(options ...server.EmbeddedOption) BaseOption {
	return &embeddedOption{
		options: options,
	}
}

func NewBase[
	ID test_utils.TableIDTypes,
	IDBUILDER test_utils.ArrowIDBuilder[ID],
](t *testing.T, state *State, name string, suiteOptions ...BaseOption) *Base[ID, IDBUILDER] {
	cfg := &baseConfig{
		name: name,
	}

	for _, option := range suiteOptions {
		option.apply(cfg)
	}

	b := &Base[ID, IDBUILDER]{
		State: state,
		cfg:   cfg,
	}

	b.SetT(t)

	return b
}
