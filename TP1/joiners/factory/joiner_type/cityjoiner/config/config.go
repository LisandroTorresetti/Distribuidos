package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"tp1/communication"
	"tp1/utils"
)

const configFilepath = "./joiners/factory/joiner_type/cityjoiner/config/config.yaml"

type CityJoinerConfig struct {
	ExchangesConfig      map[string]communication.ExchangeDeclarationConfig `yaml:"exchanges"`
	EOFQueueConfig       communication.QueueDeclarationConfig               `yaml:"eof_queue_config"`
	CityHandlerQueue     communication.QueueDeclarationConfig               `yaml:"output_city_handler_queue"`
	InputExchanges       map[string]string                                  `yaml:"input_exchanges"`
	ThresholdAvgDistance float64                                            `yaml:"threshold_avg_distance"`
	EOFType              string                                             `yaml:"eof_type"`
	City                 string
	ID                   string
}

func LoadConfig() (*CityJoinerConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var weatherConfig CityJoinerConfig
	err = yaml.Unmarshal(configFile, &weatherConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing City Joiner config file: %s", err)
	}
	weatherConfig.City = os.Getenv("CITY")
	weatherConfig.ID = os.Getenv("JOINER_ID") // possible IDs: Q1, Q2, Q3, Q4

	return &weatherConfig, nil
}
