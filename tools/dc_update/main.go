package main

import (
	"flag"
	"fmt"
	"log"

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
	path := flag.String("path", "path", "Specify the path to the file ydb file.")
	flag.Parse()

	if err := checkFileExistance(*path); err != nil {
		return fmt.Errorf("checkFileExistance %w", err)
	}

	tag, err := getLatestVersion()
	if err != nil {
		return fmt.Errorf("getLatestVersion %w", err)
	}

	checksum, err := getChecksum(tag)
	if err != nil {
		return fmt.Errorf("getCheckSum %w", err)
	}

	logger.Info("values", zap.Any("path", path), zap.Any("tag", tag), zap.Any("checksum", checksum))
	log.Println(path, tag, checksum)

	for _, pathToComposes := range pathesToComposes {
		fullPath := *path + pathToComposes

		newImage := fmt.Sprintf("ghcr.io/ydb-platform/fq-connector-go:%s@%s", tag, checksum)

		if err = walkDockerCompose(fullPath, newImage, logger); err != nil {
			return fmt.Errorf("walkDockerCompose %w", err)
		}
	}

	return nil
}
