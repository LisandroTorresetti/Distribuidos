package factory

import (
	"fmt"
	"tp1/communication"
	"tp1/workers/factory/worker_type/weather"
	weatherConfig "tp1/workers/factory/worker_type/weather/config"
)

const (
	weatherWorker = "weather-worker"
	tripsWorker   = "trips-worker"
)

type IWorker interface {
	GetID() int
	GetType() string
	GetRoutingKeys() []string
	GetEOFString() string
	DeclareQueues() error
	DeclareExchanges() error
	ProcessInputMessages() error
	Kill() error
}

// NewWorker initialize a worker of some type.
// Possible worker types are: weather-worker, trips-worker, stations-worker
func NewWorker(workerType string) (IWorker, error) {
	rabbitMQ, err := communication.NewRabbitMQ()
	if err != nil {
		return nil, err
	}

	if workerType == weatherWorker {
		cfg, err := weatherConfig.LoadConfig()
		if err != nil {
			return nil, fmt.Errorf("[method: InitWorker][status: error] error getting Weather Worker config: %w", err)
		}
		return weather.NewWeatherWorker(cfg, rabbitMQ), nil
	}

	return nil, fmt.Errorf("[method: InitWorker][status: error] Invalid worker type %s", workerType)
}
