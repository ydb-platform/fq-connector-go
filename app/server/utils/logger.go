package utils

import (
	"fmt"
	"io"
	"testing"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

// TODO: it's better to do this in GRPC middleware

func AnnotateLogger(l *zap.Logger, method string, dsi *api_common.TDataSourceInstance) *zap.Logger {
	logger := l.With(zap.String("method", method))

	if dsi != nil {
		logger = logger.With(
			zap.String("data_source_kind", api_common.EDataSourceKind_name[int32(dsi.Kind)]),
			zap.String("host", dsi.Endpoint.Host),
			zap.Uint32("port", dsi.Endpoint.Port),
			zap.String("database", dsi.Database),
			zap.Bool("use_tls", dsi.UseTls),
			zap.String("protocol", dsi.Protocol.String()),
			// TODO: can we print just a login without a password?
		)
	}

	return logger
}

func LogCloserError(logger *zap.Logger, closer io.Closer, msg string) {
	if err := closer.Close(); err != nil {
		logger.Error(msg, zap.Error(err))
	}
}

func NewLoggerFromConfig(cfg *config.TLoggerConfig) (*zap.Logger, error) {
	if cfg == nil {
		return NewDefaultLogger()
	}

	loggerCfg := zap.NewProductionConfig()
	loggerCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	loggerCfg.Encoding = "console"
	loggerCfg.Level.SetLevel(convertToZapLogLevel(cfg.GetLogLevel()))

	zapLogger, err := loggerCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("new logger: %w", err)
	}

	return zapLogger, nil
}

func NewDefaultLogger() (*zap.Logger, error) {
	return zap.Must(zap.NewDevelopment()), nil
}

func NewTestLogger(t *testing.T) *zap.Logger { return zaptest.NewLogger(t) }

func DumpReadSplitsResponse(logger *zap.Logger, resp *api_service_protos.TReadSplitsResponse) {
	switch t := resp.GetPayload().(type) {
	case *api_service_protos.TReadSplitsResponse_ArrowIpcStreaming:
		if dump := resp.GetArrowIpcStreaming(); dump != nil {
			logger.Debug("response", zap.Int("arrow_blob_length", len(dump)))
		}
	case *api_service_protos.TReadSplitsResponse_ColumnSet:
		for i := range t.ColumnSet.Data {
			data := t.ColumnSet.Data[i]
			meta := t.ColumnSet.Meta[i]

			logger.Debug("response", zap.Int("column_id", i), zap.String("meta", meta.String()), zap.String("data", data.String()))
		}
	default:
		panic(fmt.Sprintf("unexpected message type %v", t))
	}
}

func SelectToFields(slct *api_service_protos.TSelect) []zap.Field {
	result := []zap.Field{
		zap.Any("from", slct.From),
		zap.Any("what", slct.What),
		zap.Any("where", slct.Where),
	}

	return result
}

type QueryLoggerFactory struct {
	enableQueryLogging bool
}

func NewQueryLoggerFactory(cfg *config.TLoggerConfig) QueryLoggerFactory {
	enabled := cfg.GetEnableSqlQueryLogging()

	return QueryLoggerFactory{enableQueryLogging: enabled}
}

func (f *QueryLoggerFactory) Make(logger *zap.Logger) QueryLogger {
	return QueryLogger{Logger: logger, enabled: f.enableQueryLogging}
}

type QueryLogger struct {
	*zap.Logger
	enabled bool
}

func (ql *QueryLogger) Dump(query string, args ...any) {
	if !ql.enabled {
		return
	}

	logFields := []zap.Field{zap.String("query", query)}
	if len(args) > 0 {
		logFields = append(logFields, zap.Any("args", args))
	}

	ql.Debug("execute SQL query", logFields...)
}

func convertToZapLogLevel(lvl config.ELogLevel) zapcore.Level {
	switch lvl {
	case config.ELogLevel_TRACE, config.ELogLevel_DEBUG:
		return zapcore.DebugLevel
	case config.ELogLevel_INFO:
		return zapcore.InfoLevel
	case config.ELogLevel_WARN:
		return zapcore.WarnLevel
	case config.ELogLevel_ERROR:
		return zapcore.ErrorLevel
	case config.ELogLevel_FATAL:
		return zapcore.FatalLevel
	}

	return zapcore.InvalidLevel
}
