package utils

import (
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

// SQLFormatterDefault contains the most general implementations of some of
// SQLFormatter methods reflecting "standard" SQL that can be met
type SQLFormatterDefault struct{}

// RenderSelectQueryText default implementation doesn't take into account splitting.
// It's suitable for datasource that treat the whole external table as a single split.
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

func (SQLFormatterDefault) FormatStartsWith(_, _ string) (string, error) {
	return "", common.ErrUnimplementedOperation
}

func (SQLFormatterDefault) FormatEndsWith(_, _ string) (string, error) {
	return "", common.ErrUnimplementedOperation
}

func (SQLFormatterDefault) FormatContains(_, _ string) (string, error) {
	return "", common.ErrUnimplementedOperation
}

func (SQLFormatterDefault) FormatRegexp(_, _ string) (string, error) {
	return "", common.ErrUnimplementedOperation
}

func (SQLFormatterDefault) FormatIf(_, _, _ string) (string, error) {
	return "", common.ErrUnimplementedOperation
}

func (SQLFormatterDefault) FormatCast(_ string, _ *Ydb.Type) (string, error) {
	return "", common.ErrUnimplementedOperation
}

func (SQLFormatterDefault) TransformPredicateComparison(src *api_service_protos.TPredicate_TComparison) (
	*api_service_protos.TPredicate_TComparison, error) {
	return src, nil
}
