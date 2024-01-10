package utils

import (
	"fmt"

	"github.com/wI2L/jsondiff"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func ProtobufDifference(expected, actual proto.Message) (string, error) {
	if proto.Equal(expected, actual) {
		return "", nil
	}

	opts := protojson.MarshalOptions{
		Indent: "   ",
	}

	expectedDump, err := opts.Marshal(expected)
	if err != nil {
		return "", fmt.Errorf("marshal expected: %w", err)
	}

	actualDump, err := opts.Marshal(actual)
	if err != nil {
		return "", fmt.Errorf("marshal actual: %w", err)
	}

	patch, err := jsondiff.CompareJSON(actualDump, expectedDump)
	if err != nil {
		return "", fmt.Errorf("compare json: %w", err)
	}

	return patch.String(), nil
}

func MustProtobufDifference(expected, actual proto.Message) string {
	out, err := ProtobufDifference(expected, actual)
	if err != nil {
		panic(err)
	}

	return out
}
