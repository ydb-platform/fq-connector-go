package utils

import (
	"context"
	"runtime"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/fq-connector-go/common"
)

func getCallStackFunctionNames() []string {
	var functionNames []string

	pc := make([]uintptr, 20)

	n := runtime.Callers(2, pc)
	if n == 0 {
		return functionNames
	}

	pc = pc[:n]
	frames := runtime.CallersFrames(pc)

	for {
		frame, more := frames.Next()

		functionNames = append(functionNames, frame.Function)

		if !more {
			break
		}
	}

	return functionNames
}

func getTestName() string {
	functionNames := getCallStackFunctionNames()
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
