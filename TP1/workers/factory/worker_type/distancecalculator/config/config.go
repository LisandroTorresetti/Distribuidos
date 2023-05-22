package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"strconv"
	"tp1/communication"
	"tp1/utils"
)

const configFilepath = "./workers/factory/worker_type/distancecalculator/config/config.yaml"

type DistanceCalculatorConfig struct {
	EOFType            string                                  `yaml:"eof_type"`
	PreviousStage      string                                  `yaml:"previous_stage"`
	NextStage          string                                  `yaml:"next_stage"`
	AmountOfPartitions int                                     `yaml:"amount_of_partitions"`
	OutputExchange     communication.ExchangeDeclarationConfig `yaml:"exchange_output_city_handler"`
	EOFQueueConfig     communication.QueueDeclarationConfig    `yaml:"eof_queue_config"`
	InputQueueConfig   communication.QueueDeclarationConfig    `yaml:"input_queue"`
	QualityOfService   communication.QualityOfServiceConfig    `yaml:"quality_of_service"`
	City               string
	ID                 int
}

func LoadConfig() (*DistanceCalculatorConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var distanceCalculatorConfig DistanceCalculatorConfig
	err = yaml.Unmarshal(configFile, &distanceCalculatorConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing Distance Calculator config file: %s", err)
	}
	distanceCalculatorConfig.City = os.Getenv("CITY")
	distanceCalculatorConfig.ID, _ = strconv.Atoi(os.Getenv("WORKER_ID"))

	return &distanceCalculatorConfig, nil
}
