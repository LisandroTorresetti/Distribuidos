package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"tp1/communication"
	"tp1/utils"
)

const configFilepath = "./queryhandlers/factory/handler_type/rainhandler/config/config.yaml"

type RainHandlerConfig struct {
	InputQueue     communication.QueueDeclarationConfig `yaml:"input_queue"`
	EOFQueueConfig communication.QueueDeclarationConfig `yaml:"eof_queue_config"`
	OutputQueue    communication.QueueDeclarationConfig `yaml:"output_response_queue"`
	EOFType        string                               `yaml:"eof_type"`
	PreviousStage  string                               `yaml:"previous_stage"`
	//City             string ToDo: licha creo que no necesitamos esto
	//ID               string
}

func LoadConfig() (*RainHandlerConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var rainhandlerConfig RainHandlerConfig
	err = yaml.Unmarshal(configFile, &rainhandlerConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing Rain Handler config file: %s", err)
	}
	//rainhandlerConfig.City = os.Getenv("CITY")
	//rainhandlerConfig.ID = os.Getenv("JOINER_ID")

	return &rainhandlerConfig, nil
}
