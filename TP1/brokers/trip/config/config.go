package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"tp1/utils"
)

const configFilepath = "../brokers/trip/config/config.yaml"

// tripValidColumns contains the index of each field to analyze
type tripValidColumns struct {
	StartDate        int `yaml:"start_date"`
	StartStationCode int `yaml:"start_station_code"`
	EndDate          int `yaml:"start_date"`
	EndStationCode   int `yaml:"start_station_code"`
	Duration         int `yaml:"duration"`
	YearID           int `yaml:"year_id"`
}

type TripConfig struct {
	RainfallThreshold   float64          `yaml:"rainfall_threshold"`
	ValidColumnsIndexes tripValidColumns `yaml:"valid_columns"`
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

	return &tripConfig, nil
}
