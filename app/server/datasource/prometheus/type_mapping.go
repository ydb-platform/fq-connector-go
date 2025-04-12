package prometheus

import (
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

const (
	timestampColumn = "timestamp"
	valueColumn     = "value"
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
		Name: timestampColumn,
		Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TIMESTAMP}},
	}, {
		Name: valueColumn,
		Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE}},
	}}...)

	return ydbColumns
}

func toPromTime(t time.Time) int64 {
	return int64(model.TimeFromUnixNano(t.UnixNano()))
}
