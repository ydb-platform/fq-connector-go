package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func getVersion() (string, error) {
	cmd := exec.Command("curl", link)

	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	var releases []Release

	json.Unmarshal(output, &releases)

	return releases[0].Name, nil
}

func check_path(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	}

	return false
}

func walkDockerCompose(rootPath string, newImage string) error {
	walkFunc := func(path string, info os.FileInfo, err error) error {
		if info != nil && info.IsDir() {
			composeFilePath := filepath.Join(path, "docker-compose.yml")

			if _, err := os.Stat(composeFilePath); err == nil {
				changeDockerCompose(composeFilePath, newImage)
				return nil
			}

		}

		return nil
	}

	if err := filepath.Walk(rootPath, walkFunc); err != nil {

		return err
	}

	return nil
}

func changeDockerCompose(path string, newImage string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string
	counter := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "image:") {
			counter++
			if counter == 2 {
				line = "    image: " + newImage
			}
		}
		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	file.Close()

	err = os.WriteFile("docker-file.yml", []byte(strings.Join(lines, "\n")), 0644)
	if err != nil {
		return err
	}

	fmt.Printf("Updated %s\n", path)
	return nil
}
