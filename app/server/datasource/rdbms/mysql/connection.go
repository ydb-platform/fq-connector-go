package mysql

import (
	"context"

	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

type Connection struct {
	logger common.QueryLogger
}

func (Connection) Close() error {
	return nil
}

func (c Connection) Query(_ context.Context, query string, args ...any) (rdbms_utils.Rows, error) {
	c.logger.Dump(query, args...)

	return rows{}, nil
}
