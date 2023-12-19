package rdbms

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/clickhouse"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/postgresql"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"
)

func TestSchemaBuilder(t *testing.T) {
	type nameToType struct {
		name    string
		ydbType *Ydb.Type
	}

	type testCase struct {
		name                string
		typeMapper          utils.TypeMapper
		supportedTypesMatch []nameToType
		unsupportedTypes    []nameToType
	}

	testCases := []testCase{
		{
			name:       "PostgreSQL",
			typeMapper: postgresql.NewTypeMapper(),
			supportedTypesMatch: []nameToType{
				{
					"bigint",
					&Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}},
							},
						},
					},
				},
				{
					"text",
					&Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
							},
						},
					},
				},
			},
			unsupportedTypes: []nameToType{
				{"time", nil}, // yet unsupported
			},
		},
		{
			name:       "ClickHouse",
			typeMapper: clickhouse.NewTypeMapper(),
			supportedTypesMatch: []nameToType{
				{"Int32", &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}}},
				{"String", &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}}},
			},
			unsupportedTypes: []nameToType{
				{"UUID", nil}, // yet unsupported
			},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Positive_%s", tc.name), func(t *testing.T) {
			tc := tc
			sb := rdbms_utils.NewSchemaBuilder(tc.typeMapper, &api_service_protos.TTypeMappingSettings{})

			for num, supportedType := range tc.supportedTypesMatch {
				require.NoError(
					t,
					sb.AddColumn(fmt.Sprintf("suppTypeCol%d", num),
						supportedType.name)) // supported
			}

			for num, unsuppType := range tc.unsupportedTypes {
				require.NoError(
					t,
					sb.AddColumn(fmt.Sprintf("unsuppTypeCol%d", num),
						unsuppType.name)) // yet unsupported
			}

			logger := utils.NewTestLogger(t)
			schema, err := sb.Build(logger)
			require.NoError(t, err)
			require.NotNil(t, schema)

			require.Len(t, schema.Columns, len(tc.supportedTypesMatch))

			for num, supportedType := range tc.supportedTypesMatch {
				require.Equal(t, schema.Columns[num].Name, fmt.Sprintf("suppTypeCol%d", num))
				require.True(
					t,
					proto.Equal(
						schema.Columns[num].Type,
						supportedType.ydbType,
					),
					schema.Columns[num].Type)
			}
		})

		t.Run(fmt.Sprintf("EmptyTable_%s", tc.name), func(t *testing.T) {
			tc := tc
			sb := rdbms_utils.NewSchemaBuilder(tc.typeMapper, &api_service_protos.TTypeMappingSettings{})

			for num, unsuppType := range tc.unsupportedTypes {
				require.NoError(
					t,
					sb.AddColumn(
						fmt.Sprintf("unsuppTypeCol%d", num),
						unsuppType.name)) // yet unsupported
			}

			schema, err := sb.Build(utils.NewTestLogger(t))
			require.NoError(t, err)
			require.NotNil(t, schema)
			require.Len(t, schema.Columns, 0)
		})
	}

	t.Run("NonExistingTable", func(t *testing.T) {
		sb := &rdbms_utils.SchemaBuilder{}
		schema, err := sb.Build(utils.NewTestLogger(t))
		require.ErrorIs(t, err, utils.ErrTableDoesNotExist)
		require.Nil(t, schema)
	})
}
