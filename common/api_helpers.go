package common

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/ipc"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func ListSplitsResponsesToSplits(in []*api_service_protos.TListSplitsResponse) []*api_service_protos.TSplit {
	out := make([]*api_service_protos.TSplit, 0, len(in))

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

func ReadResponsesToArrowRecords(responses []*api_service_protos.TReadSplitsResponse) ([]arrow.Record, error) {
	var out []arrow.Record

	for _, resp := range responses {
		buf := bytes.NewBuffer(resp.GetArrowIpcStreaming())

		reader, err := ipc.NewReader(buf)
		if err != nil {
			return nil, fmt.Errorf("new reader: %w", err)
		}

		for reader.Next() {
			record := reader.Record()

			record.Retain()
			out = append(out, record)
		}

		reader.Release()
	}

	return out, nil
}

func ExtractErrorFromReadResponses(responses []*api_service_protos.TReadSplitsResponse) error {
	for _, resp := range responses {
		if !IsSuccess(resp.Error) {
			return NewSTDErrorFromAPIError(resp.Error)
		}
	}

	return nil
}
