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

func generateTSelect_TWhatForEmptyColumnsRequest() *api_service_protos.TSelect_TWhat { // TODO maybe global static?
	counterYdbTypeId := &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}

	countYdbType := &Ydb.Type{
		Type: counterYdbTypeId,
	}

	column := &Ydb.Column{
		Name: "",
		Type: countYdbType,
	}

	selectWhatItemColumn := &api_service_protos.TSelect_TWhat_TItem_Column{
		Column: column,
	}

	selectWhatItem := &api_service_protos.TSelect_TWhat_TItem{
		Payload: selectWhatItemColumn,
	}

	return &api_service_protos.TSelect_TWhat{
		Items: []*api_service_protos.TSelect_TWhat_TItem{selectWhatItem},
	}
}

func formatSelectColumns(
	formatter SQLFormatter,
	selectWhat *api_service_protos.TSelect_TWhat,
	tableName string,
	fakeZeroOnEmptyColumnsSet bool,
) (string, *api_service_protos.TSelect_TWhat, error) {
	// SELECT $columns FROM $from
	if tableName == "" {
		return "", nil, common.ErrEmptyTableName
	}

	var sb strings.Builder

	sb.WriteString("SELECT ")

	columns, err := selectWhatToYDBColumns(selectWhat)
	if err != nil {
		return "", nil, fmt.Errorf("convert Select.What.Items to Ydb.Columns: %w", err)
	}

	var newSelectWhat *api_service_protos.TSelect_TWhat

	// for the case of empty column set select some constant for constructing a valid sql statement
	if len(columns) == 0 {
		if !fakeZeroOnEmptyColumnsSet {
			return "", nil, fmt.Errorf("empty columns set")
		}

		sb.WriteString("0")

		newSelectWhat = generateTSelect_TWhatForEmptyColumnsRequest()
	} else {
		for i, column := range columns {
			sb.WriteString(formatter.SanitiseIdentifier(column.GetName()))

			if i != len(columns)-1 {
				sb.WriteString(", ")
			}
		}

		newSelectWhat = selectWhat
	}

	sb.WriteString(" FROM ")
	sb.WriteString(formatter.SanitiseIdentifier(tableName))

	return sb.String(), newSelectWhat, nil
}
