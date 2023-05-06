package communication

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

const rabbitUrl = "amqp://guest:guest@rabbit:5672/"

type RabbitMQ struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

// NewRabbitMQ constructor for RabbitMQ. This function returns a RabbitMQ
// with connections already established.
func NewRabbitMQ() (*RabbitMQ, error) {
	connection, err := amqp.Dial(rabbitUrl)
	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, err
	}

	return &RabbitMQ{
		connection: connection,
		channel:    channel,
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
func (r *RabbitMQ) PublishMessageInQueue(ctx context.Context, queueName string, message []byte) error {
	return r.channel.PublishWithContext(ctx,
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         message,
		},
	)
}

// PublishMessageInExchange publish a message in a given exchange with a given routing key
func (r *RabbitMQ) PublishMessageInExchange(ctx context.Context, exchange string, routingKey string, message []byte) error {
	return r.channel.PublishWithContext(ctx,
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		},
	)
}

// GetExchangeConsumer returns a consumer for a given exchange. The consumer is bound to the given routing keys
func (r *RabbitMQ) GetExchangeConsumer(exchange string, routingKeys []string) (<-chan amqp.Delivery, error) {
	anonymousQueue, err := r.channel.QueueDeclare(
		"",
		true,
		false,
		true,
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("error declaring anonymous queue: %w", err)
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
			return nil, fmt.Errorf("error binding routing key %s: %w", routingKey, err)
		}
	}

	consumer, err := r.channel.Consume(
		anonymousQueue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("error getting consumer for exchange %s: %w", exchange, err)
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
