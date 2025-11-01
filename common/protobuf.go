package common //nolint:revive

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func ProtobufToJSON(m proto.Message, multiline bool, indent string) ([]byte, error) {
	marshaler := protojson.MarshalOptions{
		Multiline:       multiline,
		Indent:          indent,
		EmitUnpopulated: false,
		UseProtoNames:   true,
	}

	prettyJSON, err := marshaler.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal protobuf message to JSON: %v", err)
	}

	return prettyJSON, nil
}

func MustProtobufToJSONString(m proto.Message, multiline bool, indent string) string {
	json, err := ProtobufToJSON(m, multiline, indent)
	if err != nil {
		panic(err)
	}

	return string(json)
}
