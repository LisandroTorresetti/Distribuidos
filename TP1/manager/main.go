package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

// InitLogger Receives the log level to be set in logrus as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	customFormatter := &log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   false,
	}
	log.SetFormatter(customFormatter)
	log.SetLevel(level)
	return nil
}

func initEOFConfig() (eofConfig, error) {
	errorMsg := "invalid amount of %s, must be numeric and greater than 0"
	weatherFilters, err := strconv.Atoi(os.Getenv("WEATHER_FILTERS"))
	if err != nil || weatherFilters < 0 {
		return eofConfig{}, fmt.Errorf(errorMsg, "weather filters")
	}

	stationFilters, err := strconv.Atoi(os.Getenv("STATION_FILTERS"))
	if err != nil || stationFilters < 0 {
		return eofConfig{}, fmt.Errorf(errorMsg, "station filters")
	}

	tripFilters, err := strconv.Atoi(os.Getenv("TRIP_FILTERS"))
	if err != nil || tripFilters < 0 {
		return eofConfig{}, fmt.Errorf(errorMsg, "trip filters")
	}

	return eofConfig{
		AmountOfWeatherFilters: weatherFilters,
		AmountOfStationFilters: stationFilters,
		AmountOfTripFilters:    tripFilters,
	}, nil
}

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "DEBUG"
	}

	if err := InitLogger(logLevel); err != nil {
		log.Fatalf("%s", err)
	}

	/*config, err := initEOFConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}*/

}
