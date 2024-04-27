package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"text/template"

	"go.uber.org/zap"

	"encoding/json"

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

type versionsInfo struct {
	tag       string
	goVersion string
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
	var (
		data     versionData
		err      error
		filepath string
		homeDir  string
	)

	if len(os.Args) != 2 {
		return fmt.Errorf("wrong args")
	}

	switch os.Args[1] {
	case "arc":
		data, err = getArcVersion()
		if err != nil {
			return fmt.Errorf("get version: %w", err)
		}

		homeDir, err = os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("get home dir: %w", err)
		}

		filepath = homeDir + "/arcadia/vendor/github.com/ydb-platform/fq-connector-go/app/version/version_init.go"
	default:
		data, err = getGitVersion()
		if err != nil {
			return fmt.Errorf("get version: %w", err)
		}

		filepath = "./app/version/version_init.go"
	}

	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}

	defer file.Close()

	t := template.Must(template.New("version").Parse(tmpl))

	err = t.Execute(file, data)
	if err != nil {
		return fmt.Errorf("template execute: %w", err)
	}

	logger.Info("Version init file generated successfully!")

	return nil
}

func getArcVersion() (versionData, error) {
	var data versionData

	commitHash, err := execCommand("arc", "log", "-n", "1", "--pretty={commit}")
	if err != nil {
		return data, fmt.Errorf("commitHash exec command: %w", err)
	}

	branch, err := execCommand("bash", "-c", "arc branch | grep \\* | cut -d ' ' -f2")

	if err != nil {
		return data, fmt.Errorf("branch exec command: %w", err)
	}

	commitMessage, err := execCommand("arc", "log", "-n", "1", "--pretty={message}")
	if err != nil {
		return data, fmt.Errorf("commitMessage exec command: %w", err)
	}

	author, err := execCommand("arc", "log", "-n", "1", "--pretty={author}")
	if err != nil {
		return data, fmt.Errorf("commitMessage exec command: %w", err)
	}

	commitDate, err := execCommand("arc", "log", "-n", "1", "--pretty={date}")
	if err != nil {
		return data, fmt.Errorf("commitMessage exec command: %w", err)
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

	versions, err := getTagAndGoVersion()
	if err != nil {
		return data, fmt.Errorf("getVersions: %w", err)
	}

	commitHash = strings.TrimSpace(commitHash)
	commitMessage = strings.TrimSpace(commitMessage)
	username = strings.TrimSpace(username)
	buildLocation = strings.TrimSpace(buildLocation)
	hostname = strings.TrimSpace(hostname)
	hostInfo = strings.TrimSpace(hostInfo)
	author = strings.TrimSpace(author)
	commitDate = strings.TrimSpace(commitDate)
	branch = strings.TrimSpace(branch)
	versions.tag = strings.TrimSpace(versions.tag)
	versions.goVersion = strings.TrimSpace(versions.goVersion)

	data = versionData{
		CommitHash:    commitHash,
		CommitMessage: commitMessage,
		CommitDate:    commitDate,
		Username:      username,
		BuildLocation: buildLocation,
		Hostname:      hostname,
		HostInfo:      hostInfo,
		PathToGo:      pathToGo,
		Author:        author,
		Branch:        branch,
		GoVersion:     versions.goVersion,
		Tag:           versions.tag,
	}

	return data, nil
}

func getGitVersion() (versionData, error) {
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

	// usr, err := user.Current()
	// if err != nil {
	// 	return "", fmt.Errorf("failed to get current user: %v", err)
	// }
	// homeDir := usr.HomeDir

	// fullPath := filepath.Join(homeDir, "arcadia")

	cmd.Stderr = &stderr
	// cmd.Dir = fullPath

	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("cmd output: %s", stderr.String())
	}

	return string(output), nil
}

func getTagAndGoVersion() (versionsInfo, error) {
	var (
		versions versionsInfo
		result   map[string]any
	)

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return versions, fmt.Errorf("get home dir: %w", err)
	}

	filepath := homeDir + "/arcadia/vendor/github.com/ydb-platform/fq-connector-go/.yo.snapshot.json"

	data, err := os.ReadFile(filepath)
	if err != nil {
		return versions, fmt.Errorf("read file %w", err)
	}

	err = json.Unmarshal(data, &result)
	if err != nil {
		return versions, fmt.Errorf("json unmarshall %w", err)
	}

	versions.tag = result["Version"].(string)
	versions.goVersion = result["GoVersion"].(string)

	return versions, nil
}
