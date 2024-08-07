package oracle

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/array"
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
	"github.com/ydb-platform/fq-connector-go/tests/suite/waiter"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"golang.org/x/exp/constraints"
	"google.golang.org/protobuf/proto"
)

func checkUserInitialized[
	T constraints.Integer,
	K test_utils.ArrowIDBuilder[T],
](
	s *suite.Base[T, K],
	dsiSrc *api_common.TDataSourceInstance,
) error {
	dsi := proto.Clone(dsiSrc).(*api_common.TDataSourceInstance)

	// read some table
	resp, err := s.Connector.
		ClientBuffering().
		DescribeTable(
			context.Background(),
			dsi,
			nil,
			"it's not important",
		)

	s.Require().NoError(err)

	switch resp.Error.Status {
	case Ydb.StatusIds_UNAUTHORIZED,
		Ydb.StatusIds_UNAVAILABLE,
		Ydb.StatusIds_INTERNAL_ERROR:
		return waiter.ErrUserNotInitialized
	}

	return nil
}

func checkTableInitialized[
	T constraints.Integer,
	K test_utils.ArrowIDBuilder[T],
](
	s *suite.Base[T, K],
	dsiSrc *api_common.TDataSourceInstance,
	tableName string,
) error {
	dsi := proto.Clone(dsiSrc).(*api_common.TDataSourceInstance)

	// read some table
	resp, err := s.Connector.ClientBuffering().DescribeTable(context.Background(), dsi, nil, tableName)
	s.Require().NoError(err)

	switch resp.Error.Status {
	case Ydb.StatusIds_NOT_FOUND:
		return waiter.ErrTableNotInitialized
	}

	return nil
}

func checkTablesInitialized[
	T constraints.Integer,
	K test_utils.ArrowIDBuilder[T],
](
	s *suite.Base[T, K],
	dsiSrc *api_common.TDataSourceInstance,
	tableNames []string,
) error {
	var err error
	for _, tableName := range tableNames {
		err = checkTableInitialized(s, dsiSrc, tableName)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkDbInitialized[
	T constraints.Integer,
	K test_utils.ArrowIDBuilder[T],
](
	s *suite.Base[T, K],
	dsiSrc *api_common.TDataSourceInstance,
) error {
	fmt.Printf("DB CHECK\n")
	err := checkUserInitialized(s, dsiSrc)
	if err != nil {
		return err
	}

	return checkTablesInitialized(s, dsiSrc, []string{"PRIMITIVES"})
}

var _ waiter.DataSourceRetrierFuncs = (*OracleRetrier)(nil)

type OracleRetrier struct {
	b          *suite.Base[int64, *array.Int64Builder]
	dataSource *datasource.DataSource
}

func (r *OracleRetrier) Op() error {
	var err error
	for _, dsi := range r.dataSource.Instances {
		err = checkDbInitialized(r.b, dsi)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *OracleRetrier) IsRetriableError(err error) bool {
	if errors.Is(err, waiter.ErrUserNotInitialized) {
		return true
	} else if errors.Is(err, waiter.ErrTableNotInitialized) {
		return true
	}
	return false
}

func newOracleRetrierFuncs(b *suite.Base[int64, *array.Int64Builder]) *OracleRetrier {
	ds, err := deriveDataSourceFromDockerCompose(b.EndpointDeterminer)
	b.Require().NoError(err)

	return &OracleRetrier{
		b:          b,
		dataSource: ds,
	}
}
