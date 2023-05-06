package factory

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"tp1/domain/communication"
	"tp1/workers/factory/worker_type/trip"
	tripConfig "tp1/workers/factory/worker_type/trip/config"
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
	DeclareQueues(channel *amqp.Channel) error
	DeclareExchanges(channel *amqp.Channel) error
	ProcessInputMessages(channel *amqp.Channel) error
	GetRabbitConfig(workerType string, configKey string) (communication.RabbitMQ, bool)
	ProcessData(ctx context.Context, channel *amqp.Channel, data string) error // ToDo: maybe we dont need it
}

// NewWorker initialize a worker of some type.
// Possible worker types are: weather-worker, trips-worker, stations-worker
func NewWorker(workerType string) (IWorker, error) {
	if workerType == weatherWorker {
		cfg, err := weatherConfig.LoadConfig()
		if err != nil {
			return nil, fmt.Errorf("[method: InitWorker][status: error] error getting Weather Worker config: %w", err)
		}
		return weather.NewWeatherWorker(cfg), nil
	}

	if workerType == tripsWorker {
		cfg, err := tripConfig.LoadConfig()
		if err != nil {
			return nil, fmt.Errorf("[method: InitWorker][status: error] error getting Weather Worker config: %w", err)
		}
		return trip.NewTripWorker(cfg), nil
	}

	return nil, fmt.Errorf("[method: InitWorker][status: error] Invalid worker type %s", workerType)
}
