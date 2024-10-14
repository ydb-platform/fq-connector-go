package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type release struct {
	Tag        string `json:"name"`
	ZipballURL string `json:"zipball_url"`
	TarballURL string `json:"tarball_url"`
	Commit     commit `json:"commit"`
	NodeID     string `json:"node_id"`
}
type commit struct {
	SHA string `json:"sha"`
	URL string `json:"url"`
}

func getLatestVersion() (string, error) {
	client := &http.Client{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	link := "https://api.github.com/repos/ydb-platform/fq-connector-go/tags"

	req, err := http.NewRequestWithContext(ctx, "GET", link, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("http new request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("http client do %w", err)
	}
	defer resp.Body.Close()

	var releases []release

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("io read all: %w", err)
	}

	if err = json.Unmarshal(body, &releases); err != nil {
		return "", fmt.Errorf("json unmarshal: %w", err)
	}

	return releases[0].Tag, nil
}

func getChecksum(tag string) (string, error) {
	baseLink := "https://github.com/ydb-platform/fq-connector-go/pkgs/container/fq-connector-go/tags"

	params := map[string]string{
		"tag": tag,
	}

	link, err := generateURL(baseLink, params)
	if err != nil {
		return "", fmt.Errorf("generate url: %w", err)
	}

	client := &http.Client{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", link, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("http new request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("http client do %w", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)

	var checksum string

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "sha256") {
			fmt.Println(line)
			line = strings.Split(line, "<span>")[1]
			line = strings.Split(line, "</span>")[0]
			checksum = line

			break
		}
	}

	if checksum == "" {
		return "", fmt.Errorf("no checksum found by latest tag")
	}

	return checksum, nil
}

func checkFileExistance(path string) error {
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("os stat: %w", err)
	}

	return nil
}

func walkDockerCompose(logger *zap.Logger, rootPath string, newImage string) error {
	walkFunc := func(path string, info os.FileInfo, err error) error {
		if info != nil && info.IsDir() {
			composeFilePath := filepath.Join(path, "docker-compose.yml")

			if err := checkFileExistance(composeFilePath); err == nil {
				if err = changeDockerCompose(logger, composeFilePath, newImage); err != nil {
					return fmt.Errorf("change docker compose: %w", err)
				}

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

func changeDockerCompose(logger *zap.Logger, path string, newImage string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("os openfile: %w", err)
	}

	defer func() {
		if err = file.Close(); err != nil {
			log.Println("error closing file %w", err)
		}
	}()

	var data map[any]any

	decoder := yaml.NewDecoder(file)

	if err = decoder.Decode(&data); err != nil {
		return fmt.Errorf("decode file: %w", err)
	}

	if services, ok := data["services"].(map[string]any); ok {
		if _, ok := services["fq-connector-go"].(map[string]any); !ok {
			return fmt.Errorf("error finding fq-connector-go in services")
		}

		fqConnectorGo := services["fq-connector-go"].(map[string]any)
		fqConnectorGo["image"] = newImage
	} else {
		return fmt.Errorf("error finding services in file: %s", path)
	}

	var buf bytes.Buffer

	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)

	if err = encoder.Encode(data); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	err = os.WriteFile(path, buf.Bytes(), 0644)
	if err != nil {
		return fmt.Errorf("os write file: %w", err)
	}

	logger.Info("Updated", zap.String("path", path))

	return nil
}

func generateURL(baseURL string, params map[string]string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("url parse: %w", err)
	}

	q := u.Query()
	for key, value := range params {
		q.Set(key, value)
	}

	u.RawQuery = q.Encode()

	return u.String(), nil
}
