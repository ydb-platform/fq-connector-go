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

	labelsPrefix = "labels."
	metaPrefix   = "meta."

	levelTraceValue = "TRACE"
	levelDebugValue = "DEBUG"
	levelInfoValue  = "INFO"
	levelWarnValue  = "WARN"
	levelErrorValue = "ERROR"
	levelFatalValue = "FATAL"
)
