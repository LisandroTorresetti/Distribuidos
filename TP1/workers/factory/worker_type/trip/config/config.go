package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"strconv"
	"tp1/communication"
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

type TripWorkerConfig struct {
	ValidColumnsIndexes tripValidColumns                                   `yaml:"valid_columns"`
	ExchangesConfig     map[string]communication.ExchangeDeclarationConfig `yaml:"exchanges"`
	EOFQueueConfig      communication.QueueDeclarationConfig               `yaml:"eof_queue_config"`
	InputExchange       string                                             `yaml:"input_exchange"`
	IncludeYears        []int                                              `yaml:"include_years"`
	IncludeCities       []string                                           `yaml:"include_cities"`
	DataDelimiter       string                                             `yaml:"data_delimiter"`
	DataFieldDelimiter  string                                             `yaml:"data_field_delimiter"`
	EndBatchMarker      string                                             `yaml:"end_batch_marker"`
	City                string
	ID                  int
}

func LoadConfig() (*TripWorkerConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var tripConfig TripWorkerConfig
	err = yaml.Unmarshal(configFile, &tripConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing trip config file: %s", err)
	}

	tripConfig.City = os.Getenv("CITY")
	tripConfig.ID, _ = strconv.Atoi(os.Getenv("WORKER_ID"))

	return &tripConfig, nil
}
