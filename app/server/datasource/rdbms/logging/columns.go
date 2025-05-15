package logging

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
)

var externalToInternalColumnName = map[string]string{
	levelColumnName:       levelColumnName,
	messageColumnName:     messageColumnName,
	timestampColumnName:   timestampColumnName,
	projectColumnName:     jsonPayloadColumnName,
	serviceColumnName:     jsonPayloadColumnName,
	clusterColumnName:     jsonPayloadColumnName,
	jsonPayloadColumnName: jsonPayloadColumnName,
	labelsColumnName:      jsonPayloadColumnName,
	metaColumnName:        jsonPayloadColumnName,
}

var internalColumnTypes = map[string]*Ydb.Type{
	levelColumnName:       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
	messageColumnName:     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
	timestampColumnName:   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
	jsonPayloadColumnName: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_JSON)),
}
