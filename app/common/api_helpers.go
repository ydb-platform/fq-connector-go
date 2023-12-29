package common

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func ListSplitsResponsesToSplits(in []*api_service_protos.TListSplitsResponse) []*api_service_protos.TSplit {
	var out []*api_service_protos.TSplit

	for _, resp := range in {
		out = append(out, resp.Splits...)
	}

	return out
}

func SchemaToSelectWhatItems(
	schema *api_service_protos.TSchema,
	whitelist map[string]struct{},
) *api_service_protos.TSelect_TWhat {
	out := &api_service_protos.TSelect_TWhat{}

	for _, column := range schema.Columns {
		pick := true

		if whitelist != nil {
			if _, exists := whitelist[column.Name]; !exists {
				pick = false
			}
		}

		if pick {
			item := &api_service_protos.TSelect_TWhat_TItem{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: column,
				},
			}

			out.Items = append(out.Items, item)
		}
	}

	return out
}
