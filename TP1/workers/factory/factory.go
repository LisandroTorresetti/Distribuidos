package factory

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"tp1/domain/communication"
	"tp1/workers/factory/worker_type/weather"
	"tp1/workers/factory/worker_type/weather/config"
)

const (
	weatherWorker = "weather-worker"
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
		weatherConfig, err := config.LoadConfig()
		if err != nil {
			return nil, fmt.Errorf("[method: InitWorker][status: error] error getting Weather Worker config: %w", err)
		}
		return weather.NewWeatherWorker(weatherConfig), nil
	}
	return nil, fmt.Errorf("[method: InitWorker][status: error] Invalid worker type %s", workerType)
}
