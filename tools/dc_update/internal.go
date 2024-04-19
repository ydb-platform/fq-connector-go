package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type Release struct {
	Tag        string `json:"name"`
	ZipballURL string `json:"zipball_url"`
	TarballURL string `json:"tarball_url"`
	Commit     struct {
		SHA string `json:"sha"`
		URL string `json:"url"`
	} `json:"commit"`
	NodeID string `json:"node_id"`
}

func getLatestVersion() (string, error) {
	client := &http.Client{}

	req, err := http.NewRequest("GET", link, nil)
	if err != nil {
		return "", fmt.Errorf("http newRequest: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("http client do %w", err)
	}
	defer resp.Body.Close()

	var releases []Release

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("io readAll: %w", err)
	}

	if err = json.Unmarshal(body, &releases); err != nil {
		return "", fmt.Errorf("json unmarshal: %w", err)
	}

	return releases[0].Tag, nil
}

func getChecksum(tag string) (string, error) {
	link := fmt.Sprintf("https://github.com/ydb-platform/fq-connector-go/pkgs/container/fq-connector-go/204229242?tag=%s", tag)

	client := &http.Client{}

	req, err := http.NewRequest("GET", link, nil)
	if err != nil {
		return "", fmt.Errorf("http newRequest: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("http client do %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("io readAll: %w", err)
	}

	lines := strings.Split(string(body), "\n")

	var checksum string

	for _, line := range lines {
		if strings.Contains(line, "sha256") {
			line = strings.Split(line, "<code>")[1]
			line = strings.Split(line, "</code>")[0]
			checksum = line
			break
		}
	}

	if checksum == "" {
		return "", fmt.Errorf("no checksum found by lattest tag")
	}

	return checksum, nil
}

func checkFileExistance(path string) error {
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("os stat: %w", err)
	}

	return nil
}

func walkDockerCompose(rootPath string, newImage string) error {
	walkFunc := func(path string, info os.FileInfo, err error) error {
		if info != nil && info.IsDir() {
			composeFilePath := filepath.Join(path, "docker-compose.yml")

			if err := checkFileExistance(composeFilePath); err == nil {
				changeDockerCompose(composeFilePath, newImage)
				return nil
			}
		}

		return nil
	}

	if err := filepath.Walk(rootPath, walkFunc); err != nil {
		return fmt.Errorf("filepath walk: %w", err)
	}

	return nil
}

func changeDockerCompose(path string, newImage string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("os openfile: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "ghcr.io/ydb-platform/fq-connector-go") {
			line = "    image: " + newImage
		}
		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner err: %w", err)
	}
	file.Close()

	err = os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0644)
	if err != nil {
		return fmt.Errorf("os writefile: %w", err)
	}

	log.Printf("Updated %s\n", path)
	return nil
}
