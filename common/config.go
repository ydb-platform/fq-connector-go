package common

import (
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

type AppConfig interface {
	*config.TClientConfig | *config.TBenchmarkConfig
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
