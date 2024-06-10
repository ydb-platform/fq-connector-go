package utils

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

func selectWhatToYDBColumns(selectWhat *api_service_protos.TSelect_TWhat) ([]*Ydb.Column, error) {
	var columns []*Ydb.Column

	for i, item := range selectWhat.Items {
		column := item.GetColumn()
		if column == nil {
			return nil, fmt.Errorf("item #%d (%v) is not a column", i, item)
		}

		columns = append(columns, column)
	}

	return columns, nil
}

func formatSelectColumns(
	formatter SQLFormatter,
	selectWhat *api_service_protos.TSelect_TWhat,
	tableName string,
	fakeZeroOnEmptyColumnsSet bool,
) (string, error) {
	// SELECT $columns FROM $from
	if tableName == "" {
		return "", common.ErrEmptyTableName
	}

	var sb strings.Builder

	sb.WriteString("SELECT ")

	columns, err := selectWhatToYDBColumns(selectWhat)
	if err != nil {
		return "", fmt.Errorf("convert Select.What.Items to Ydb.Columns: %w", err)
	}

	// for the case of empty column set select some constant for constructing a valid sql statement
	if len(columns) == 0 {
		if !fakeZeroOnEmptyColumnsSet {
			return "", fmt.Errorf("empty columns set")
		}

		sb.WriteString("0")

	} else {
		for i, column := range columns {
			sb.WriteString(formatter.SanitiseIdentifier(column.GetName()))

			if i != len(columns)-1 {
				sb.WriteString(", ")
			}
		}
	}

	sb.WriteString(" FROM ")
	sb.WriteString(formatter.SanitiseIdentifier(tableName))

	return sb.String(), nil
}
