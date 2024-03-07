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

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type Base struct {
	testify_suite.Suite
	*State
	Connector *server.Embedded
	name      string
}

func (b *Base) BeforeTest(_, testName string) {
	fmt.Printf("\n>>>>>>>>>> TEST STARTED: %s/%s <<<<<<<<<<\n\n", b.name, testName)
}

func (b *Base) TearDownTest() {
	if b.T().Failed() {
		// Do not launch other tests if this test failed
		b.T().FailNow()
	}
}

func (b *Base) BeforeSuite(_ string) {
	fmt.Printf("\n>>>>>>>>>> SUITE STARTED: %s <<<<<<<<<<\n", b.name)
}

func (b *Base) SetupSuite() {
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
	)
	b.Require().NoError(err)
	b.Connector.Start()
}

func (b *Base) TearDownSuite() {
	b.Connector.Stop()

	fmt.Printf("\n>>>>>>>>>> Suite stopped: %s <<<<<<<<<<\n", b.name)
}

type validateTableOptions struct {
	typeMappingSettings *api_service_protos.TTypeMappingSettings
	predicate           *api_service_protos.TPredicate
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

func (b *Base) ValidateTable(ds *datasource.DataSource, table *datasource.Table, customOptions ...ValidateTableOption) {
	for _, dsi := range ds.Instances {
		b.doValidateTable(table, dsi, customOptions...)
	}
}

func (b *Base) doValidateTable(table *datasource.Table, dsi *api_common.TDataSourceInstance, customOptions ...ValidateTableOption) {
	options := newDefaultValidateTableOptions()
	for _, option := range customOptions {
		option.apply(options)
	}

	b.Require().NotEmpty(table.Name)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	b.Require().Len(readSplitsResponses, 1)

	records, err := common.ReadResponsesToArrowRecords(readSplitsResponses)
	b.Require().NoError(err)

	// verify data
	table.MatchRecords(b.T(), records, schema)
}

func NewBase(t *testing.T, state *State, name string) *Base {
	b := &Base{
		State: state,
		name:  name,
	}

	b.SetT(t)

	return b
}
