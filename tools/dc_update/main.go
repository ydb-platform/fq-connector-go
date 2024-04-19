package main

import (
	"flag"
	"fmt"
	"log"
)

const link = "https://api.github.com/repos/ydb-platform/fq-connector-go/tags"

var pathesToComposes = [3]string{"/ydb/library/yql/providers/generic/connector/tests/datasource", "/ydb/library/yql/providers/generic/connector/tests/join", "/ydb/tests/fq/generic"}

func main() {
	log.Println(run())
}

func run() error {
	path := flag.String("path", "path", "Specify the path to the file ydb file.")
	flag.Parse()

	if err := checkFileExistance(*path); err != nil {
		return err
	}

	tag, err := getLatestVersion()
	if err != nil {
		return err
	}
	checksum, err := getChecksum(tag)
	if err != nil {
		return err
	}

	log.Println(path, tag, checksum)

	for _, pathToComposes := range pathesToComposes {
		fullPath := *path + pathToComposes
		fmt.Println(fullPath)

		newImage := fmt.Sprintf("ghcr.io/ydb-platform/fq-connector-go:%s@%s", tag, checksum)

		if err = walkDockerCompose(fullPath, newImage); err != nil {
			return err
		}
	}

	return nil
}
