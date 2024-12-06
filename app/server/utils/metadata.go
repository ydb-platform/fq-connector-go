package utils

import (
	"context"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/fq-connector-go/common"
)

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

// metainfo is a container for a useful parameters and tags for a user request
// which must be used for log annotation
type metainfo struct {
	testName string // used only in integration tests (Go)
}

type loggerKey int

const (
	loggerKeyRequest loggerKey = iota
)

func extractMetadata(ctx context.Context) metainfo {
	var m metainfo

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return m
	}

	testNames := md[common.TestName]
	if len(testNames) != 0 {
		m.testName = testNames[0]
	}

	return m
}

func UnaryServerMetadata(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = insertMetadataToContext(ctx, logger, info.FullMethod)

		return handler(ctx, req)
	}
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func StreamServerMetadata(logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := insertMetadataToContext(stream.Context(), logger, info.FullMethod)

		return handler(srv, &wrappedStream{stream, ctx})
	}
}

func trimMethod(info string) string {
	parts := strings.Split(info, "/")
	lastWord := parts[len(parts)-1]

	return lastWord
}

func insertMetadataToContext(serverContext context.Context, logger *zap.Logger, fullMethod string) context.Context {
	metainfo := extractMetadata(serverContext)

	method := trimMethod(fullMethod)

	fields := []zap.Field{
		zap.String("method", method),
	}

	if metainfo.testName != "" {
		fields = append(fields, zap.String("test_name", metainfo.testName))
	}

	newLogger := logger.With(fields...)

	ctx := context.WithValue(serverContext, loggerKeyRequest, newLogger)

	return ctx
}

func LoggerMustFromContext(ctx context.Context) *zap.Logger {
	logger := ctx.Value(loggerKeyRequest).(*zap.Logger)
	return logger
}
