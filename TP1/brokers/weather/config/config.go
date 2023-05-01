package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"tp1/domain/communication"
	"tp1/utils"
)

const configFilepath = "/weather/config/config.yaml"

// weatherValidColumns contains the index of each field to analyze
type weatherValidColumns struct {
	Date         int                                 `yaml:"date"`
	Rainfall     int                                 `yaml:"rainfall"`
	InputQueues  map[string][]communication.RabbitMQ `yaml:"input_queues"`
	OutputQueues map[string][]communication.RabbitMQ `yaml:"output_queues"`
}
type WeatherConfig struct {
	RainfallThreshold       float64             `yaml:"rainfall_threshold"`
	ValidColumnsIndexes     weatherValidColumns `yaml:"valid_columns"`
	FinishProcessingMessage string
}

func LoadConfig() (*WeatherConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var weatherConfig WeatherConfig
	err = yaml.Unmarshal(configFile, &weatherConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing weather config file: %s", err)
	}
	weatherConfig.FinishProcessingMessage = os.Getenv("FINISH_PROCESSING_MESSAGE")

	return &weatherConfig, nil
}
