package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"tp1/communication"
	"tp1/utils"
)

const configFilepath = "./queryhandlers/factory/handler_type/duplicateshandler/config/config.yaml"

type DuplicatesHandlerConfig struct {
	InputQueue     communication.QueueDeclarationConfig `yaml:"input_queue"`
	EOFQueueConfig communication.QueueDeclarationConfig `yaml:"eof_queue_config"`
	OutputQueue    communication.QueueDeclarationConfig `yaml:"output_response_queue"`
	InputQueueName string                               `yaml:"input_queue_name"`
	EOFType        string                               `yaml:"eof_type"`
	PreviousStage  string                               `yaml:"previous_stage"`
	City           string
}

func LoadConfig() (*DuplicatesHandlerConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var duplicatesHandlerConfig DuplicatesHandlerConfig
	err = yaml.Unmarshal(configFile, &duplicatesHandlerConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing Duplicates Handler config file: %s", err)
	}
	city := os.Getenv("CITY") // One node per each city
	duplicatesHandlerConfig.City = city
	duplicatesHandlerConfig.InputQueue.Name = fmt.Sprintf(duplicatesHandlerConfig.InputQueueName, city)

	return &duplicatesHandlerConfig, nil
}
