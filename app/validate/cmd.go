package validate

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/ydb-platform/fq-connector-go/app/server/config"
	"github.com/ydb-platform/fq-connector-go/common"

	sigs_yaml "sigs.k8s.io/yaml"
)

var Cmd = &cobra.Command{
	Use:   "validate",
	Short: "Config validation toolkit",
}

var helmCmd = &cobra.Command{
	Use:   "helm",
	Short: "Validate Helm configuration file",
	Run: func(cmd *cobra.Command, args []string) {
		if err := validateHelmConfigurationFile(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

const (
	fileFlag = "file"
	keyFlag  = "key"
)

func init() {
	Cmd.AddCommand(helmCmd)

	helmCmd.Flags().StringP(fileFlag, "f", "", "Path to Helm file")
	helmCmd.Flags().StringP(keyFlag, "k", "", "Key by which the Connector config is stored within the Helm file")

	if err := helmCmd.MarkFlagRequired(fileFlag); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := helmCmd.MarkFlagRequired(keyFlag); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func validateHelmConfigurationFile(cmd *cobra.Command, _ []string) error {
	file, err := cmd.Flags().GetString(fileFlag)
	if err != nil {
		return fmt.Errorf("get config flag: %v", err)
	}

	key, err := cmd.Flags().GetString(keyFlag)
	if err != nil {
		return fmt.Errorf("get key flag: %v", err)
	}

	parsedFile, err := parseYAMLFile(file)
	if err != nil {
		return fmt.Errorf("parse YAML file: %v", err)
	}

	keyPart, ok := parsedFile[key]
	if !ok {
		return fmt.Errorf("key '%s' not found in YAML file", key)
	}

	tempFile, err := ioutil.TempFile("", "connector-config-*.yaml")
	if err != nil {
		return fmt.Errorf("create temp file: %v", err)
	}

	logger := common.NewDefaultLogger()

	defer func() {
		common.LogCloserError(logger, tempFile, "close temp file")

		if errInner := os.Remove(tempFile.Name()); errInner != nil {
			logger.Error("remove temp file", zap.Error(errInner))
		}
	}()

	if err = ioutil.WriteFile(tempFile.Name(), []byte(keyPart.(string)), 0644); err != nil {
		return fmt.Errorf("write temp file: %v", err)
	}

	cfg, err := config.NewConfigFromFile(tempFile.Name())
	if err != nil {
		return fmt.Errorf("new config from YAML data: %v", err)
	}

	prettyJSON, err := common.ProtobufToJSON(cfg, true, "  ")
	if err != nil {
		return fmt.Errorf("marshal config to JSON: %v", err)
	}

	prettyYAML, err := sigs_yaml.JSONToYAML(prettyJSON)
	if err != nil {
		return fmt.Errorf("convert JSON to YAML: %v", err)
	}

	fmt.Println(string(prettyYAML))

	return nil
}

// parseYAMLFile will read a YAML file and decode it into a map
func parseYAMLFile(filename string) (map[string]any, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create a new YAML decoder
	decoder := yaml.NewDecoder(file)

	// Use an interface to hold the YAML content
	var data map[string]any

	// Decode YAML data into interface map
	err = decoder.Decode(&data)
	if err != nil {
		return nil, err
	}

	return data, nil
}
