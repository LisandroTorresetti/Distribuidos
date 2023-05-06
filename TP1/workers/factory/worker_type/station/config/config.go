package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"strconv"
	"tp1/communication"
	"tp1/utils"
)

const configFilepath = "./workers/factory/worker_type/station/config/config.yaml"

// stationValidColumns contains the index of each field to analyze
type stationValidColumns struct {
	Code      int `yaml:"code"`
	Name      int `yaml:"name"`
	Latitude  int `yaml:"latitude"`
	Longitude int `yaml:"longitude"`
	YearID    int `yaml:"year_id"` // ToDo: maybe we can delete this field. Licha
}

type StationConfig struct {
	ValidColumnsIndexes     stationValidColumns                                `yaml:"valid_columns"`
	RabbitMQConfig          map[string]map[string]communication.RabbitMQConfig `yaml:"rabbit_mq"`
	FinishProcessingMessage string
	City                    string
	ID                      int
}

func LoadConfig() (*StationConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var stationConfig StationConfig
	err = yaml.Unmarshal(configFile, &stationConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing station config file: %s", err)
	}

	stationConfig.FinishProcessingMessage = os.Getenv("FINISH_PROCESSING_MESSAGE")
	stationConfig.City = os.Getenv("CITY")
	stationConfig.ID, _ = strconv.Atoi(os.Getenv("WORKER_ID"))

	return &stationConfig, nil
}
