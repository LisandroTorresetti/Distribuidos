package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"tp1/communication"
	"tp1/utils"
)

const configFilepath = "./joiners/factory/joiner_type/yearjoiner/config/config.yaml"

type YearJoinerConfig struct {
	ExchangesConfig      map[string]communication.ExchangeDeclarationConfig `yaml:"exchanges"`
	EOFQueueConfig       communication.QueueDeclarationConfig               `yaml:"eof_queue_config"`
	YearGrouper2016Queue communication.QueueDeclarationConfig               `yaml:"output_yeargrouper2016_queue"`
	YearGrouper2017Queue communication.QueueDeclarationConfig               `yaml:"output_yeargrouper2017_queue"`
	InputExchanges       map[string]string                                  `yaml:"input_exchanges"`
	ValidYears           []int                                              `yaml:"valid_years"`
	EOFType              string                                             `yaml:"eof_type"`
	City                 string
	ID                   string
}

func LoadConfig() (*YearJoinerConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var yearConfig YearJoinerConfig
	err = yaml.Unmarshal(configFile, &yearConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing City Joiner config file: %s", err)
	}
	yearConfig.City = os.Getenv("CITY")
	yearConfig.ID = os.Getenv("JOINER_ID") // possible IDs: Q1, Q2, Q3, Q4

	return &yearConfig, nil
}
