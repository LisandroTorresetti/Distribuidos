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

type StationWorkerConfig struct {
	ValidColumnsIndexes stationValidColumns                                `yaml:"valid_columns"`
	ExchangesConfig     map[string]communication.ExchangeDeclarationConfig `yaml:"exchanges"`
	EOFQueueConfig      communication.QueueDeclarationConfig               `yaml:"eof_queue_config"`
	IncludeYears        []int                                              `yaml:"include_years"`
	IncludeCities       []string                                           `yaml:"include_cities"`
	DataDelimiter       string                                             `yaml:"data_delimiter"`
	DataFieldDelimiter  string                                             `yaml:"data_field_delimiter"`
	EndBatchMarker      string                                             `yaml:"end_batch_marker"`
	City                string
	ID                  int
}

func LoadConfig() (*StationWorkerConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var stationConfig StationWorkerConfig
	err = yaml.Unmarshal(configFile, &stationConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing station config file: %s", err)
	}

	stationConfig.City = os.Getenv("CITY")
	stationConfig.ID, _ = strconv.Atoi(os.Getenv("WORKER_ID"))

	return &stationConfig, nil
}
