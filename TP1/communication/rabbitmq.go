package communication

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
)

const rabbitUrlEnvVarName = "RABBIT_URL"

type RabbitMQ struct {
	connection                  *amqp.Connection
	channel                     *amqp.Channel
	exchangeToAnonymousQueueMap map[string]string
}

// NewRabbitMQ constructor for RabbitMQ. This function returns a RabbitMQ
// with connections already established.
func NewRabbitMQ() (*RabbitMQ, error) {
	connection, err := amqp.Dial(os.Getenv(rabbitUrlEnvVarName))
	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, err
	}

	exchangeToAnonymousQueueMap := make(map[string]string)

	return &RabbitMQ{
		connection:                  connection,
		channel:                     channel,
		exchangeToAnonymousQueueMap: exchangeToAnonymousQueueMap,
	}, nil
}

// DeclareNonAnonymousQueues declares non-anonymous queues based on the slice of configs
func (r *RabbitMQ) DeclareNonAnonymousQueues(queuesConfig []QueueDeclarationConfig) error {
	for idx := range queuesConfig {
		queueName := queuesConfig[idx].Name
		_, err := r.channel.QueueDeclare(
			queueName,
			queuesConfig[idx].Durable,
			queuesConfig[idx].DeleteWhenUnused,
			queuesConfig[idx].Exclusive,
			queuesConfig[idx].NoWait,
			nil,
		)

		if err != nil {
			return fmt.Errorf("error declaring queue %s: %w", queueName, err)
		}
	}
	return nil
}

// DeclareExchanges declare exchanges based on the slice of configs
func (r *RabbitMQ) DeclareExchanges(exchangesConfig []ExchangeDeclarationConfig) error {
	for idx := range exchangesConfig {
		exchangeName := exchangesConfig[idx].Name
		err := r.channel.ExchangeDeclare(
			exchangeName,
			exchangesConfig[idx].Type,
			exchangesConfig[idx].Durable,
			exchangesConfig[idx].AutoDeleted,
			exchangesConfig[idx].Internal,
			exchangesConfig[idx].NoWait,
			nil,
		)

		if err != nil {
			return fmt.Errorf("error declaring exchange %s: %w", exchangeName, err)
		}
	}
	return nil
}

// PublishMessageInQueue publish a message in a given queue
func (r *RabbitMQ) PublishMessageInQueue(ctx context.Context, queueName string, message []byte, contentType string) error {
	return r.channel.PublishWithContext(ctx,
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  contentType,
			Body:         message,
		},
	)
}

// PublishMessageInExchange publish a message in a given exchange with a given routing key
func (r *RabbitMQ) PublishMessageInExchange(ctx context.Context, exchange string, routingKey string, message []byte, contentType string) error {
	return r.channel.PublishWithContext(ctx,
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: contentType,
			Body:        message,
		},
	)
}

// Bind binds input exchanges with routing keys. The anonymous queues declare here are saved.
func (r *RabbitMQ) Bind(inputExchanges []string, routingKeys []string) error {
	for _, exchange := range inputExchanges {
		anonymousQueue, err := r.channel.QueueDeclare(
			"",
			true,
			false,
			true,
			false,
			nil,
		)

		if err != nil {
			return fmt.Errorf("error declaring anonymous queue: %w", err)
		}

		for _, routingKey := range routingKeys {
			err = r.channel.QueueBind(
				anonymousQueue.Name,
				routingKey,
				exchange,
				false,
				nil,
			)
			if err != nil {
				return fmt.Errorf("error binding routing key %s: %w", routingKey, err)
			}
		}
		r.exchangeToAnonymousQueueMap[exchange] = anonymousQueue.Name
	}
	return nil
}

// GetConsumerForExchange returns a consumer for the given exchange name
func (r *RabbitMQ) GetConsumerForExchange(exchangeName string) (<-chan amqp.Delivery, error) {
	queueName, ok := r.exchangeToAnonymousQueueMap[exchangeName]
	if !ok {
		return nil, fmt.Errorf("error queue not found for exchange %s", exchangeName)
	}

	consumer, err := r.channel.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("error getting consumer for exchange %s: %w", exchangeName, err)
	}

	return consumer, nil
}

// GetQueueConsumer returns a consumer for a given queue
func (r *RabbitMQ) GetQueueConsumer(queueName string) (<-chan amqp.Delivery, error) {
	consumer, err := r.channel.Consume(
		queueName,
		"",
		true, // ToDo: check this, maybe needs to be false
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("error getting consumer for queue %s: %w", queueName, err)
	}

	return consumer, nil
}

// KillBadBunny close RabbitMQ's connection and channel
func (r *RabbitMQ) KillBadBunny() error {
	err := r.channel.Close()
	if err != nil {
		return fmt.Errorf("error closing RabbitMQ channel: %w", err)
	}

	err = r.connection.Close()
	if err != nil {
		return fmt.Errorf("error closing RabbitMQ connection: %w", err)
	}

	return nil
}
