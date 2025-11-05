package utils //nolint:revive

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/fq-connector-go/common"
)

func getTestName() string {
	functionNames := common.GetCallStackFunctionNames()
	if len(functionNames) == 0 {
		return ""
	}

	for _, functionName := range functionNames {
		if strings.Contains(functionName, "*Suite") {
			split := strings.Split(functionName, ".")

			return split[len(split)-1]
		}
	}

	return ""
}

func NewContextWithTestName() context.Context {
	md := metadata.Pairs(common.TestName, getTestName())

	return metadata.NewOutgoingContext(context.Background(), md)
}
