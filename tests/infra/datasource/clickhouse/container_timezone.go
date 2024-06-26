package clickhouse

import (
	"fmt"
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

func extractClickHouseTimezoneFromMap(data map[any]any) (string, error) {
	var timezoneStr string

	if services, ok := data["services"].(map[any]any); ok {
		if _, ok = services["clickhouse"].(map[any]any); !ok {
			return "", fmt.Errorf("error finding clickhouse in file")
		}

		clickhouseServ := services["clickhouse"].(map[any]any)

		if _, ok = clickhouseServ["environment"].(map[any]any); !ok {
			return "", fmt.Errorf("error finding environment in file")
		}

		environment := clickhouseServ["environment"].(map[any]any)

		timezoneStr, ok = environment["TZ"].(string)
		if !ok {
			return "", fmt.Errorf("error finding TZ in file")
		}
	} else {
		return "", fmt.Errorf("error finding services in file")
	}

	return timezoneStr, nil
}

func parseClickHouseTimzoneFromDockerFile(path string) (string, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return "", fmt.Errorf("os openfile: %w", err)
	}

	defer func() {
		if err = file.Close(); err != nil {
			log.Println("error closing file %w", err)
		}
	}()
	decoder := yaml.NewDecoder(file)

	var data map[any]any

	if err = decoder.Decode(&data); err != nil {
		return "", fmt.Errorf("decode file: %w", err)
	}

	timezoneStr, err := extractClickHouseTimezoneFromMap(data)
	if err != nil {
		return "", err
	}

	return timezoneStr, nil
}

func mustGetClickHouseDockerTimezone(path string) *time.Location {
	timezoneStr, err := parseClickHouseTimzoneFromDockerFile(path)
	if err != nil {
		panic(err)
	}

	locat, err := time.LoadLocation(timezoneStr)
	if err != nil {
		panic(err)
	}

	return locat
}
