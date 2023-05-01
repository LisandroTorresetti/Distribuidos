package brokers

import (
	"fmt"
	"tp1/brokers/station"
	stationConfig "tp1/brokers/station/config"
	"tp1/brokers/trip"
	tripConfig "tp1/brokers/trip/config"
	"tp1/brokers/weather"
	weatherConfig "tp1/brokers/weather/config"
)

type IBroker interface {
	ProcessData(string) error
}

type BrokerType string

var (
	weatherBroker  BrokerType = "weather-broker"
	stationsBroker BrokerType = "stations-broker"
	tripBroker     BrokerType = "trip-broker"
)

func NewBroker(broker BrokerType, delimiter string) (IBroker, error) {
	switch broker {
	case weatherBroker:
		return createWeatherBroker(delimiter)
	case tripBroker:
		return createTripBroker(delimiter)
	case stationsBroker:
		return createStationBroker(delimiter)
	default:
		return nil, fmt.Errorf("invalid broker type: %s. Must be: %s, %s or %s", broker, weatherBroker, stationsBroker, tripBroker)
	}
}

func createWeatherBroker(delimiter string) (*weather.WeatherBroker, error) {
	brokerConfig, err := weatherConfig.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("error getting Weather Broker config: %w", err)
	}
	return weather.NewWeatherBroker(delimiter, brokerConfig), nil
}

func createStationBroker(delimiter string) (*station.StationBroker, error) {
	brokerConfig, err := stationConfig.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("error getting Station Broker config: %w", err)
	}
	return station.NewStationBroker(delimiter, brokerConfig), nil
}

func createTripBroker(delimiter string) (*trip.TripBroker, error) {
	brokerConfig, err := tripConfig.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("error getting Trip Broker config: %w", err)
	}
	return trip.NewTripBroker(delimiter, brokerConfig), nil
}
