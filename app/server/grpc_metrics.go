package server

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/library/go/core/metrics"
	"github.com/ydb-platform/fq-connector-go/library/go/core/metrics/solomon"
)

type errorGetter interface {
	GetError() *api_service_protos.TError
}

// maybeRegisterStatusCode is used to fill sensor representing the number of succesfull/failed responses.
// There are two kinds of errors in the system:
// 1. transport errors (e. g. client stream interrupted, so we failed to send response)
// 2. logical errors
func maybeRegisterStatusCode(statusCount metrics.CounterVec, opName string, streamingMethod bool, response any, err error) {
	grpcStatusCode := status.Code(err)

	// transport error happened
	if grpcStatusCode != codes.OK {
		statusCount.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
			"status":   grpcStatusCode.String(),
		}).Inc()

		return
	}

	// check possible logical error
	eg, ok := response.(errorGetter)
	if !ok {
		panic(fmt.Sprintf("failed to cast response of type %T to errorGetter", response))
	}

	if eg.GetError() == nil {
		// All unary methods responses must have errors filled
		if !streamingMethod {
			panic(fmt.Sprintf("message of type %T has no filled error", response))
		}

		// Streaming methods do not fill errors in the middle of the stream, but only in terminating message
		return
	}

	ydbStatus := eg.GetError().Status

	var ydbStatusStr string
	if ydbStatus != Ydb.StatusIds_SUCCESS {
		// convert YDB status code to string
		ydbStatusStr = ydbStatus.String()
	} else {
		// return "OK" for backward compatibility with Solomon monitoring plots
		ydbStatusStr = "OK"
	}

	statusCount.With(map[string]string{
		"protocol": "grpc",
		"endpoint": opName,
		"status":   ydbStatusStr,
	}).Inc()
}

func UnaryServerMetrics(logger *zap.Logger, registry metrics.Registry) grpc.UnaryServerInterceptor {
	requestCount := registry.CounterVec("requests_total", []string{"protocol", "endpoint"})
	requestDuration := registry.DurationHistogramVec(
		"request_duration_seconds",
		metrics.MakeExponentialDurationBuckets(250*time.Microsecond, 1.5, 35),
		[]string{"protocol", "endpoint"})
	panicsCount := registry.CounterVec("panics_total", []string{"protocol", "endpoint"})
	inflightRequests := registry.GaugeVec("inflight_requests", []string{"protocol", "endpoint"})
	statusCount := registry.CounterVec("status_total", []string{"protocol", "endpoint", "status"})
	requestBytes := registry.CounterVec("request_bytes", []string{"protocol", "endpoint"})
	responseBytes := registry.CounterVec("response_bytes", []string{"protocol", "endpoint"})

	solomon.Rated(requestCount)
	solomon.Rated(requestDuration)
	solomon.Rated(panicsCount)
	solomon.Rated(statusCount)
	solomon.Rated(requestBytes)
	solomon.Rated(responseBytes)

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ any, err error) {
		deferFunc := func(startTime time.Time, opName string) {
			requestDuration.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}).RecordDuration(time.Since(startTime))

			inflightRequests.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}).Add(-1)

			if p := recover(); p != nil {
				panicsCount.With(map[string]string{
					"protocol": "grpc",
					"endpoint": opName,
				}).Inc()

				stacktrace := make([]byte, 1024)
				runtime.Stack(stacktrace, false)
				logger.Error("panic occurred", zap.Any("error", p), zap.String("stacktrace", fmt.Sprint(string(stacktrace))))

				// return transport error to the client
				err = status.Errorf(codes.Internal, fmt.Sprintf("Server paniced: %v", p))
			}
		}

		opName := info.FullMethod

		requestBytes.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Add(int64(proto.Size(req.(proto.Message))))

		requestCount.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Inc()

		inflightRequests.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Add(1)

		startTime := time.Now()
		defer deferFunc(startTime, opName)

		resp, err := handler(ctx, req)

		maybeRegisterStatusCode(statusCount, opName, false, resp, err)

		responseBytes.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Add(int64(proto.Size(resp.(proto.Message))))

		return resp, err
	}
}

func StreamServerMetrics(logger *zap.Logger, registry metrics.Registry) grpc.StreamServerInterceptor {
	streamCount := registry.CounterVec("streams_total", []string{"protocol", "endpoint"})
	streamDuration := registry.DurationHistogramVec(
		"stream_duration_seconds",
		metrics.MakeExponentialDurationBuckets(250*time.Microsecond, 1.5, 35),
		[]string{"protocol", "endpoint"},
	)
	inflightStreams := registry.GaugeVec("inflight_streams", []string{"protocol", "endpoint"})
	panicsCount := registry.CounterVec("stream_panics_total", []string{"protocol", "endpoint"})
	sentStreamMessages := registry.CounterVec("sent_stream_messages_total", []string{"protocol", "endpoint"})
	receivedBytes := registry.CounterVec("received_bytes", []string{"protocol", "endpoint"})
	sentBytes := registry.CounterVec("sent_bytes", []string{"protocol", "endpoint"})
	statusCount := registry.CounterVec("stream_status_total", []string{"protocol", "endpoint", "status"})
	receivedStreamMessages := registry.CounterVec("received_stream_messages_total", []string{"protocol", "endpoint"})

	solomon.Rated(streamCount)
	solomon.Rated(streamDuration)
	solomon.Rated(panicsCount)
	solomon.Rated(sentStreamMessages)
	solomon.Rated(receivedStreamMessages)
	solomon.Rated(receivedBytes)
	solomon.Rated(sentBytes)
	solomon.Rated(statusCount)

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		deferFunc := func(startTime time.Time, opName string) {
			streamDuration.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}).RecordDuration(time.Since(startTime))

			inflightStreams.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}).Add(-1)

			if p := recover(); p != nil {
				panicsCount.With(map[string]string{
					"protocol": "grpc",
					"endpoint": opName,
				}).Inc()

				stacktrace := make([]byte, 1024)
				runtime.Stack(stacktrace, false)
				logger.Error("panic occurred", zap.Any("error", p), zap.String("stacktrace", fmt.Sprint(string(stacktrace))))

				// return transport error to the client
				err := status.Errorf(codes.Internal, fmt.Sprintf("Server paniced: %v", p))
				_ = ss.SendMsg(err)
			}
		}

		opName := info.FullMethod

		streamCount.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Inc()

		inflightStreams.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Add(1)

		startTime := time.Now()
		defer deferFunc(startTime, opName)

		return handler(srv, serverStreamWithMessagesCount{
			ServerStream: ss,
			sentStreamMessages: sentStreamMessages.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}),
			receivedStreamMessages: receivedStreamMessages.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}),
			sentBytes: sentBytes.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}),
			receivedBytes: receivedBytes.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}),
			statusCount: statusCount,
			method:      opName,
		})
	}
}

type serverStreamWithMessagesCount struct {
	grpc.ServerStream
	sentStreamMessages     metrics.Counter
	receivedStreamMessages metrics.Counter
	sentBytes              metrics.Counter
	receivedBytes          metrics.Counter
	statusCount            metrics.CounterVec
	method                 string
}

func (s serverStreamWithMessagesCount) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)

	if err == nil {
		s.sentStreamMessages.Inc()
		s.sentBytes.Add(int64(proto.Size(m.(proto.Message))))
	}

	maybeRegisterStatusCode(s.statusCount, s.method, true, m, err)

	return err
}

func (s serverStreamWithMessagesCount) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)

	if err == nil {
		s.receivedStreamMessages.Inc()
		s.receivedBytes.Add(int64(proto.Size(m.(proto.Message))))
	}

	// No need to register errors while receiving requests
	// maybeRegisterStatusCode(s.statusCount, s.method, true, m, err)

	return err
}
