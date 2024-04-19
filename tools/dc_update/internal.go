package main

import (
	"bufio"
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

	req, err := http.NewRequestWithContext(ctx, "GET", link, nil)
	if err != nil {
		return "", fmt.Errorf("http newRequest: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf("http client do %w", ctx.Err())
		}

		return "", fmt.Errorf("http client do %w", err)
	}
	defer resp.Body.Close()

	var releases []release

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
	baseLink := fmt.Sprintf("https://github.com/ydb-platform/fq-connector-go/pkgs/container/fq-connector-go/204229242")

	params := map[string]string{
		tag: tag,
	}

	link, err := generateUrl(baseLink, params)
	if err != nil {
		return "", fmt.Errorf("generateUrl: %w", err)
	}

	client := &http.Client{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", link, nil)
	if err != nil {
		return "", fmt.Errorf("http newRequest: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf("http client do %w", ctx.Err())
		}

		return "", fmt.Errorf("http client do %w", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)

	var checksum string

	for scanner.Scan() {
		line := scanner.Text()
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

func walkDockerCompose(rootPath string, newImage string, logger *zap.Logger) error {
	walkFunc := func(path string, info os.FileInfo, err error) error {
		if info != nil && info.IsDir() {
			composeFilePath := filepath.Join(path, "docker-compose.yml")

			if err := checkFileExistance(composeFilePath); err == nil {
				if err = changeDockerCompose(composeFilePath, newImage, logger); err != nil {
					return fmt.Errorf("changeDockerCompose: %w", err)
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

func changeDockerCompose(path string, newImage string, logger *zap.Logger) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("os openfile: %w", err)
	}

	defer func() {
		if err = file.Close(); err != nil {
			log.Println("error closing file %w", err)
		}
	}()

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

	err = os.WriteFile("docker-file.yaml", []byte(strings.Join(lines, "\n")), 0644)
	if err != nil {
		return fmt.Errorf("os writefile: %w", err)
	}

	logger.Info("Updated", zap.Any("path", path))

	return nil
}

func generateUrl(baseUrl string, params map[string]string) (string, error) {
	u, err := url.Parse(baseUrl)
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
