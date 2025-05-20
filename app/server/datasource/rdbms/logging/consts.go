package logging

const (
	clusterColumnName     = "cluster"
	jsonPayloadColumnName = "json_payload"
	labelsColumnName      = "labels"
	levelColumnName       = "level"
	messageColumnName     = "message"
	metaColumnName        = "meta"
	projectColumnName     = "project"
	serviceColumnName     = "service"
	timestampColumnName   = "timestamp"
	hostnameColumnName    = "hostname"
	spanIDColumnName      = "span.id"
	traceIDColumnName     = "trace.id"

	levelTraceValue = "TRACE"
	levelDebugValue = "DEBUG"
	levelInfoValue  = "INFO"
	levelWarnValue  = "WARN"
	levelErrorValue = "ERROR"
	levelFatalValue = "FATAL"
)
