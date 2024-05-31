package main

import (
	"flag"
	"fmt"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/common"
)

var pathesToComposes = [3]string{"/ydb/library/yql/providers/generic/connector/tests/datasource",
	"/ydb/library/yql/providers/generic/connector/tests/join",
	"/ydb/tests/fq/generic"}

func main() {
	logger := common.NewDefaultLogger()

	err := run(logger)
	if err != nil {
		logger.Error("run", zap.Error(err))
	}
}

func run(logger *zap.Logger) error {
	path := flag.String("path", "path", "Specify the path to ydb file.")
	flag.Parse()

	if err := checkFileExistance(*path); err != nil {
		return fmt.Errorf("check file existence %w", err)
	}

	tag, err := getLatestVersion()
	if err != nil {
		return fmt.Errorf("get latest version %w", err)
	}

	checksum, err := getChecksum(tag)
	if err != nil {
		return fmt.Errorf("get check sum %w", err)
	}

	logger.Info("values", zap.String("path", *path), zap.String("tag", tag), zap.String("checksum", checksum))

	for _, pathToComposes := range pathesToComposes {
		fullPath := *path + pathToComposes

		newImage := fmt.Sprintf("ghcr.io/ydb-platform/fq-connector-go:%s@%s", tag, checksum)

		if err = walkDockerCompose(logger, fullPath, newImage); err != nil {
			return fmt.Errorf("walk docker compose %w", err)
		}
	}

	return nil
}
