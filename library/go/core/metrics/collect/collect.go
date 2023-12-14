package collect

import (
	"context"

	"github.com/ydb-platform/fq-connector-go/library/go/core/metrics"
)

type Func func(ctx context.Context, r metrics.Registry, c metrics.CollectPolicy)
