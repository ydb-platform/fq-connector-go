package utils

import (
	"strings"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

// SQLFormatterDefault contains the most general implementations of some of
// SQLFormatter methods reflecting "standard" SQL that can be met
type SQLFormatterDefault struct{}

// SelectQueryRender default implementation doesn't take into account splitting
func (SQLFormatterDefault) RenderSelectQueryText(
	parts *SelectQueryParts,
	_ *api_service_protos.TSplit,
) (string, error) {
	var sb strings.Builder

	sb.WriteString("SELECT ")
	sb.WriteString(parts.SelectClause)
	sb.WriteString(" FROM ")
	sb.WriteString(parts.FromClause)

	if parts.WhereClause != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(parts.WhereClause)
	}

	return sb.String(), nil
}
