package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"tp1/communication"
	"tp1/utils"
)

const configFilepath = "./queryhandlers/factory/handler_type/cityhandler/config/config.yaml"

type CityHandlerConfig struct {
	InputQueue        communication.QueueDeclarationConfig `yaml:"input_queue"`
	EOFQueueConfig    communication.QueueDeclarationConfig `yaml:"eof_queue_config"`
	OutputQueue       communication.QueueDeclarationConfig `yaml:"output_response_queue"`
	EOFType           string                               `yaml:"eof_type"`
	PreviousStage     string                               `yaml:"previous_stage"`
	ThresholdDistance float64                              `yaml:"threshold_distance"`
	City              string
}

func LoadConfig() (*CityHandlerConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var cityHandlerConfig CityHandlerConfig
	err = yaml.Unmarshal(configFile, &cityHandlerConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing City Handler config file: %s", err)
	}
	cityHandlerConfig.City = os.Getenv("CITY")

	return &cityHandlerConfig, nil
}
