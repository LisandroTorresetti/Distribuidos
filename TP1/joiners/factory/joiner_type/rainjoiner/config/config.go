package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"tp1/communication"
	"tp1/utils"
)

const configFilepath = "./joiners/factory/joiner_type/rainjoiner/config/config.yaml"

type RainJoinerConfig struct {
	ExchangesConfig  map[string]communication.ExchangeDeclarationConfig `yaml:"exchanges"`
	EOFQueueConfig   communication.QueueDeclarationConfig               `yaml:"eof_queue_config"`
	RainHandlerQueue communication.QueueDeclarationConfig               `yaml:"output_rain_handler_queue"`
	InputExchanges   map[string]string                                  `yaml:"input_exchanges"`
	City             string
	ID               string
}

func LoadConfig() (*RainJoinerConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var weatherConfig RainJoinerConfig
	err = yaml.Unmarshal(configFile, &weatherConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing Rain Joiner config file: %s", err)
	}
	weatherConfig.City = os.Getenv("CITY")
	weatherConfig.ID = os.Getenv("JOINER_ID")

	return &weatherConfig, nil
}
