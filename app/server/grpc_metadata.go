package server

import (
	"context"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

type md struct {
	userID    string
	sessionID string
}

type loggerKey int

const (
	loggerKeyRequest loggerKey = iota
)

func extractMetadata(ctx context.Context) md {
	var metainfo md

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return metainfo
	}

	userIDs := md["user_id"]
	if len(userIDs) != 0 {
		metainfo.userID = userIDs[0]
	}

	sessionIDs := md["session_id"]
	if len(sessionIDs) != 0 {
		metainfo.sessionID = sessionIDs[0]
	}

	return metainfo
}

func UnaryServerMetadata(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		metainfo := extractMetadata(ctx)

		method := trimMethod(info.FullMethod)

		newLogger := logger.With(
			zap.String("user_id", metainfo.userID),
			zap.String("session_id", metainfo.sessionID),
			zap.String("method", method),
		)

		ctx = context.WithValue(ctx, loggerKeyRequest, newLogger)

		return handler(ctx, req)
	}
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func StreamServerMetadata(logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		metainfo := extractMetadata(stream.Context())

		method := trimMethod(info.FullMethod)

		newLogger := logger.With(
			zap.String("user_id", metainfo.userID),
			zap.String("session_id", metainfo.sessionID),
			zap.String("method", method),
		)

		ctx := context.WithValue(stream.Context(), loggerKeyRequest, newLogger)

		return handler(srv, &wrappedStream{stream, ctx})
	}
}

func trimMethod(info string) string {
	parts := strings.Split(info, "/")
	lastWord := parts[len(parts)-1]

	return lastWord
}
