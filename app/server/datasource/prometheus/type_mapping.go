package prometheus

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func timeSeriesToYdbSchema(l labels.Labels) []*Ydb.Column {
	ydbColumns := make([]*Ydb.Column, 0, l.Len())
	l.Range(func(label labels.Label) {
		ydbColumns = append(ydbColumns, &Ydb.Column{
			Name: label.Name,
			Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}},
		})
	})

	// All schemas contain timestamp and value
	ydbColumns = append(ydbColumns, []*Ydb.Column{{
		Name: "timestamp",
		Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TIMESTAMP}},
	}, {
		Name: "value",
		Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE}},
	}}...)

	return ydbColumns
}
