package main

import (
	"flag"
	"fmt"
	"log"
)

const link = "https://api.github.com/repos/ydb-platform/fq-connector-go/tags"
const path_to_composes = "/ydb/library/yql/providers/generic/connector/tests/datasource"

type Release struct {
	Name       string `json:"name"`
	ZipballURL string `json:"zipball_url"`
	TarballURL string `json:"tarball_url"`
	Commit     struct {
		SHA string `json:"sha"`
		URL string `json:"url"`
	} `json:"commit"`
	NodeID string `json:"node_id"`
}

func main() {
	log.Println(run())
}

func run() error {
	path := flag.String("path", "path", "Specify the path to the file ydb file.")
	checksum := flag.String("checksum", "checksum", "Specify checksum.")
	flag.Parse()

	if !check_path(*path) {
		fmt.Print("Path does not exist")
		return nil
	}

	tag, err := getVersion()
	if err != nil {
		return err
	}

	fmt.Println(path, tag, checksum)

	fullPath := *path + path_to_composes
	fmt.Println(fullPath)

	newImage := fmt.Sprintf("ghcr.io/ydb-platform/fq-connector-go:%s@sha256:%s", tag, *checksum)

	if err = walkDockerCompose(fullPath, newImage); err != nil {
		return err
	}

	return nil
}
