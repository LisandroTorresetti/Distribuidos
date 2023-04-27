package weather

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
	"tp1/brokers/weather/config"
	"tp1/domain/entities/weather"
	dataErrors "tp1/errors"
)

const dateLayout = "2023-04-22"

type WeatherBroker struct {
	config    *config.WeatherConfig
	delimiter string
}

func NewWeatherBroker(delimiter string, weatherBrokerConfig *config.WeatherConfig) *WeatherBroker {
	return &WeatherBroker{
		delimiter: delimiter,
		config:    weatherBrokerConfig,
	}
}

func (wb *WeatherBroker) ProcessData(data string) error {
	log.Infof("Processing WEATHER data: %s", data)
	weatherData, err := wb.getWeatherData(data)
	if err != nil {
		if errors.Is(err, dataErrors.ErrInvalidWeatherData) {
			return nil
		}
		return err
	}

	if wb.isValid(weatherData) {
		// ToDo: here we should send a message to rabbitMQ
		log.Infof("IS VALID %s", data)
		return nil
	}

	log.Infof("INVALID DATA %s", data)
	return nil
}

// getWeatherData returns a struct WeatherData
func (wb *WeatherBroker) getWeatherData(data string) (*weather.WeatherData, error) {
	dataSplit := strings.Split(data, wb.delimiter)
	date, err := time.Parse(dateLayout, dataSplit[wb.config.ValidColumnsIndexes.Date])
	if err != nil {
		log.Debugf("Invalid date")
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidDate, dataErrors.ErrInvalidWeatherData)
	}

	date = date.AddDate(0, 0, -1)

	rainfall, err := strconv.ParseFloat(dataSplit[wb.config.ValidColumnsIndexes.Rainfall], 64)
	if err != nil {
		log.Debugf("Invalid rainfall type")
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidRainfallType, dataErrors.ErrInvalidWeatherData)
	}

	return &weather.WeatherData{
		Date:     date,
		Rainfall: rainfall,
	}, nil
}

// isValid returns true if the following conditions are met:
// + Rainfall is greater than 30mm
func (wb *WeatherBroker) isValid(weatherData *weather.WeatherData) bool {
	return weatherData.Rainfall > wb.config.RainfallThreshold
}
