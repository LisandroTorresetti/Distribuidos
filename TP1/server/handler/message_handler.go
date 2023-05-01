package handler

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
	"tp1/domain/communication"
	"tp1/socket"
	"tp1/utils"
)

const filterQueue = "_filter_queue"

type MessageHandlerConfig struct {
	EndBatchMarker string
	FinMessages    []string
	AckMessage     string
}

type MessageHandler struct {
	config        MessageHandlerConfig
	handlerSocket *socket.Socket
	queuesConfigs map[string]communication.RabbitMQ
}

func NewMessageHandler(config MessageHandlerConfig, handlerSocket *socket.Socket, queuesConfigs map[string]communication.RabbitMQ) *MessageHandler {
	return &MessageHandler{
		config:        config,
		handlerSocket: handlerSocket,
		queuesConfigs: queuesConfigs,
	}
}

func (mh *MessageHandler) ProcessData() error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %s", err.Error())
	}
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			log.Errorf("error closing RabbitMQ connection")
		}
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %s", err.Error())
	}
	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			log.Errorf("error closing RabbitMQ channel")
		}
	}(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	targetQueue := ""

	for {
		// Wait till receive the entire message data1|data2|...|dataN|PING or x-PONG
		messageBytes, err := mh.handlerSocket.Listen(mh.config.EndBatchMarker, mh.config.FinMessages)

		if err != nil {
			log.Errorf("[server] error receiving message from a client: %s", err.Error())
			return err
		}
		message := string(messageBytes)
		log.Debugf("received message: %s", message)

		// If we received a FIN message, the response is the same FIN message
		if utils.ContainsString(message, mh.config.FinMessages) {
			err = mh.handlerSocket.Send(message)
			if err != nil {
				log.Errorf("error sending ACK to FIN message %s: %s", message, err.Error())
				return err
			}
			log.Debug("Fin message ACK sent correctly")

			err = mh.publishMessage(ctx, ch, message, targetQueue)
			if err != nil {
				log.Errorf("error publishing FIN Message: %s", err.Error())
				return err
			}
			break
		}

		if strings.Contains(message, mh.config.EndBatchMarker) {
			err = mh.handlerSocket.Send(mh.config.AckMessage)
			if err != nil {
				log.Errorf("[server] error sending ACK to client: %s", err.Error())
				return err
			}

			if targetQueue == "" {
				targetQueue = strings.SplitN(message, ",", 2)[0]
				targetQueue += filterQueue
			}

			err = mh.publishMessage(ctx, ch, message, targetQueue)
			if err != nil {
				log.Errorf("error publishing DATA message %s: %s", message, err.Error())
				return err
			}
		}
	}
	return nil
}

func (mh *MessageHandler) publishMessage(ctx context.Context, channel *amqp.Channel, message string, targetQueue string) error {
	queueConfig, ok := mh.queuesConfigs[targetQueue]
	if !ok {
		return fmt.Errorf("invalid target queue %s", targetQueue)
	}

	publishConfig := queueConfig.PublishingConfig

	return channel.PublishWithContext(ctx,
		publishConfig.Exchange,
		queueConfig.Name,
		publishConfig.Mandatory,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  publishConfig.ContentType,
			Body:         []byte(message),
		},
	)
}
