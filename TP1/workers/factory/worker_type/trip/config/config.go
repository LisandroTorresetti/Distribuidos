package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"strconv"
	"tp1/domain/communication"
	"tp1/utils"
)

const configFilepath = "./workers/factory/worker_type/trip/config/config.yaml"

// tripValidColumns contains the index of each field to analyze
type tripValidColumns struct {
	StartDate        int `yaml:"start_date"`
	StartStationCode int `yaml:"start_station_code"`
	EndDate          int `yaml:"end_date"`
	EndStationCode   int `yaml:"end_station_code"`
	Duration         int `yaml:"duration"`
	YearID           int `yaml:"year_id"`
}

type TripConfig struct {
	ValidColumnsIndexes     tripValidColumns                             `yaml:"valid_columns"`
	RabbitMQConfig          map[string]map[string]communication.RabbitMQ `yaml:"rabbit_mq"`
	FinishProcessingMessage string
	City                    string
	ID                      int
}

func LoadConfig() (*TripConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var tripConfig TripConfig
	err = yaml.Unmarshal(configFile, &tripConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing trip config file: %s", err)
	}

	tripConfig.FinishProcessingMessage = os.Getenv("FINISH_PROCESSING_MESSAGE")
	tripConfig.City = os.Getenv("CITY")
	tripConfig.ID, _ = strconv.Atoi(os.Getenv("WORKER_ID"))

	return &tripConfig, nil
}
