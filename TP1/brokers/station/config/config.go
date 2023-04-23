package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"tp1/utils"
)

const configFilepath = "../brokers/station/config/config.yaml"

// stationValidColumns contains the index of each field to analyze
type stationValidColumns struct {
	Code      int `yaml:"code"`
	Name      int `yaml:"name"`
	Latitude  int `yaml:"latitude"`
	Longitude int `yaml:"longitude"`
	YearID    int `yaml:"year_id"` // ToDo: maybe we can delete this field. Licha
}

type StationConfig struct {
	ValidColumnsIndexes stationValidColumns `yaml:"valid_columns"`
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

	return &stationConfig, nil
}
