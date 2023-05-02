package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"strconv"
	"tp1/domain/communication"
	"tp1/utils"
)

const configFilepath = "./workers/factory/worker_type/weather/config/config.yaml"

// weatherValidColumns contains the index of each field to analyze
type weatherValidColumns struct {
	Date     int `yaml:"date"`
	Rainfall int `yaml:"rainfall"`
}
type WeatherConfig struct {
	RainfallThreshold       float64                                      `yaml:"rainfall_threshold"`
	ValidColumnsIndexes     weatherValidColumns                          `yaml:"valid_columns"`
	InputQueues             map[string][]communication.RabbitMQ          `yaml:"input_queues"`
	RabbitMQConfig          map[string]map[string]communication.RabbitMQ `yaml:"rabbit_mq"`
	FinishProcessingMessage string
	City                    string
	ID                      int
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
	weatherConfig.City = os.Getenv("CITY")
	weatherConfig.ID, _ = strconv.Atoi(os.Getenv("WORKER_ID"))

	return &weatherConfig, nil
}
