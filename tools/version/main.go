package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"text/template"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/common"
)

type versionData struct {
	Branch        string
	CommitHash    string
	Tag           string
	Author        string
	CommitDate    string
	CommitMessage string
	Username      string
	BuildLocation string
	Hostname      string
	HostInfo      string
	PathToGo      string
	GoVersion     string
}

var tmpl = `
package version

func init() {
	tag = "{{ .Tag }}"
	author = "{{ .Author }}"
	commitHash = "{{ .CommitHash }}"
	branch = "{{ .Branch }}"
	commitDate = "{{ .CommitDate }}"
	commitMessage = "{{ .CommitMessage }}"
	username = "{{ .Username }}"
	buildLocation = "{{ .BuildLocation }}"
	hostname = "{{ .Hostname }}"
	hostInfo = "{{ .HostInfo }}"
	pathToGo = "{{ .PathToGo }}"
	goVersion = "{{ .GoVersion }}"
}
`

func main() {
	logger := common.NewDefaultLogger()

	err := run(logger)
	if err != nil {
		logger.Error("run", zap.Error(err))
		os.Exit(1)
	}
}

func run(logger *zap.Logger) error {
	data, err := getVersion()
	if err != nil {
		return fmt.Errorf("get version: %w", err)
	}

	file, err := os.Create("./app/version/version_init.go")
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}

	defer file.Close()

	t := template.Must(template.New("version").Parse(tmpl))

	err = t.Execute(file, data)
	if err != nil {
		return fmt.Errorf("template execute: %w", err)
	}

	logger.Info(string(data.Tag))

	logger.Info("Version init file generated successfully!")

	return nil
}

func getVersion() (versionData, error) {
	var data versionData

	branch, err := execCommand("git", "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return data, fmt.Errorf("branch exec command: %w", err)
	}

	commitHash, err := execCommand("git", "rev-parse", "HEAD")
	if err != nil {
		return data, fmt.Errorf("commitHash exec command: %w", err)
	}

	tag, err := execCommand("git", "describe", "--tags")
	if err != nil {
		return data, fmt.Errorf("tag exec command: %w", err)
	}

	author, err := execCommand("git", "log", "-1", "--pretty=format:%an")
	if err != nil {
		return data, fmt.Errorf("author exec command: %w", err)
	}

	commitDate, err := execCommand("git", "show", "-s", "--format=%cd", "--date=format:%Y-%m-%d %H:%M:%S")
	if err != nil {
		return data, fmt.Errorf("commit date exec command: %w", err)
	}

	commitMessage, err := execCommand("git", "log", "-1", "--pretty=%B")
	if err != nil {
		return data, fmt.Errorf("commit message exec command: %w", err)
	}

	username, err := os.Executable()
	if err != nil {
		return data, fmt.Errorf("username exec command: %w", err)
	}

	buildLocation, err := os.Getwd()
	if err != nil {
		return data, fmt.Errorf("build location exec command: %w", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return data, fmt.Errorf("hostname exec command: %w", err)
	}

	hostInfo, err := execCommand("uname", "-s")
	if err != nil {
		return data, fmt.Errorf("host info exec command: %w", err)
	}

	pathToGo, err := exec.LookPath("go")
	if err != nil {
		return data, fmt.Errorf("path to go exec command: %w", err)
	}

	goVersion, err := execCommand("go", "version")
	if err != nil {
		return data, fmt.Errorf("go ersion exec command: %w", err)
	}

	branch = strings.TrimSpace(branch)
	commitHash = strings.TrimSpace(commitHash)
	tag = strings.TrimSpace(tag)
	author = strings.TrimSpace(author)
	commitDate = strings.TrimSpace(commitDate)
	commitMessage = strings.TrimSpace(commitMessage)
	username = strings.TrimSpace(username)
	buildLocation = strings.TrimSpace(buildLocation)
	hostname = strings.TrimSpace(hostname)
	hostInfo = strings.TrimSpace(hostInfo)
	goVersion = strings.TrimSpace(goVersion)

	data = versionData{
		Branch:        branch,
		CommitHash:    commitHash,
		Tag:           tag,
		Author:        author,
		CommitDate:    commitDate,
		CommitMessage: commitMessage,
		Username:      username,
		BuildLocation: buildLocation,
		Hostname:      hostname,
		HostInfo:      hostInfo,
		PathToGo:      pathToGo,
		GoVersion:     goVersion,
	}

	return data, nil
}

func execCommand(command string, args ...string) (string, error) {
	var stderr bytes.Buffer

	cmd := exec.Command(command, args...)

	cmd.Stderr = &stderr

	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("cmd output: %s", stderr.String())
	}

	return string(output), nil
}
