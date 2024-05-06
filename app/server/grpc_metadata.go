package server

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/fq-connector-go/common"
)

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func extractMetadata(ctx context.Context) (string, string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", fmt.Errorf("metadata not found in incoming context")
	}

	userIDs := md["user_id"]
	if len(userIDs) == 0 {
		return "", "", fmt.Errorf("user_id not found in metadata")
	}

	userID := userIDs[0]

	sessionIDs := md["session_id"]
	if len(sessionIDs) == 0 {
		return "", "", fmt.Errorf("session_id not found")
	}

	sessionID := sessionIDs[0]

	return userID, sessionID, nil
}

func UnaryServerMetadata(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		userID, sessionID, err := extractMetadata(ctx)
		if err != nil {
			return nil, fmt.Errorf("error extracting unary metadata: %v", err)
		}

		logger = logger.With(
			zap.String("user_id", userID),
			zap.String("session_id", sessionID),
		)

		ctx = context.WithValue(ctx, "logger", logger)

		return handler(ctx, req)
	}
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func SessionStreamMetadata() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		userID, sessionID, err := extractMetadata(stream.Context())
		if err != nil {
			return fmt.Errorf("error extracting stream metadata: %v", err)
		}

		newLogger := common.NewDefaultLogger()

		newLogger = newLogger.With(
			zap.String("service", connectorServiceKey),
			zap.String("user_id", userID),
			zap.String("session_id", sessionID),
		)

		ctx := context.WithValue(stream.Context(), "logger", newLogger)

		return handler(srv, &wrappedStream{stream, ctx})
	}
}
