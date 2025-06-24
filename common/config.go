package common

import (
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"sigs.k8s.io/yaml"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

type AppConfig interface {
	*config.TServerConfig |
		*config.TClientConfig |
		*config.TBenchmarkConfig |
		*config.TObservationServerConfig
	proto.Message
}

func NewConfigFromPrototextFile[T AppConfig](configPath string, dst T) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read file %v: %w", configPath, err)
	}

	if err := prototext.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("prototext unmarshal `%v`: %w", string(data), err)
	}

	return nil
}

func NewConfigFromYAMLFile[T AppConfig](configPath string, dst T) error {
	dataYAML, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read file %v: %w", configPath, err)
	}

	// convert YAML to JSON
	dataJSON, err := yaml.YAMLToJSON(dataYAML)
	if err != nil {
		return fmt.Errorf("convert YAML to JSON: %w", err)
	}

	// than parse JSON

	unmarshaller := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}

	if err := unmarshaller.Unmarshal(dataJSON, dst); err != nil {
		return fmt.Errorf("protojson unmarshal `%v`: %w", string(dataJSON), err)
	}

	return nil
}
