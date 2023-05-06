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

type MessageHandlerConfig struct {
	EndBatchMarker string
	FinMessages    []string
	AckMessage     string
}

type MessageHandler struct {
	ID             string
	config         MessageHandlerConfig
	handlerSocket  *socket.Socket
	rabbitMQConfig map[string]communication.RabbitMQ
}

func NewMessageHandler(config MessageHandlerConfig, handlerSocket *socket.Socket, queuesConfigs map[string]communication.RabbitMQ, id string) *MessageHandler {
	return &MessageHandler{
		ID:             id,
		config:         config,
		handlerSocket:  handlerSocket,
		rabbitMQConfig: queuesConfigs,
	}
}

func (mh *MessageHandler) ProcessData() error {
	conn, err := amqp.Dial("amqp://guest:guest@rabbit:5672/")
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

	targetExchange := ""

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

			routingKey := targetExchange + ".eof"
			log.Debugf("Fin message routing key %s", routingKey)

			err = mh.publishMessage(ctx, ch, message, targetExchange, routingKey)
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

			splitMessage := strings.SplitN(message, ",", 3)
			targetExchange = splitMessage[0] // targetExchange could be: weather, trips or stations
			city := splitMessage[1]          // city could be: montreal, toronto or washington
			randomID := getRandomID()
			routingKey := fmt.Sprintf("%s.%s.%v", targetExchange, city, randomID)
			log.Debugf("Generated routing key %s", routingKey)

			err = mh.publishMessage(ctx, ch, message, targetExchange, routingKey)
			if err != nil {
				log.Errorf("error publishing DATA message %s: %s", message, err.Error())
				return err
			}
		}
	}
	return nil
}

// publishMessage publish a message in targetExchange with some routing key
// Possible routing keys are: dataType.city.id or dataType.eof
// + dataType: weather, trips, stations
// + city: toronto, montreal, washington
// + id: number > 0
func (mh *MessageHandler) publishMessage(ctx context.Context, channel *amqp.Channel, message string, targetExchange string, routingKey string) error {
	rabbitConfig, ok := mh.rabbitMQConfig[targetExchange]
	if !ok {
		return fmt.Errorf("invalid target exchange %s", targetExchange)
	}

	publishConfig := rabbitConfig.PublishingConfig
	log.Infof("[smsHandler: %s]Publishing message in exchange %s", publishConfig.Exchange, mh.ID)

	return channel.PublishWithContext(ctx,
		publishConfig.Exchange,
		routingKey,
		publishConfig.Mandatory,
		publishConfig.Immediate,
		amqp.Publishing{
			ContentType: publishConfig.ContentType,
			Body:        []byte(message),
		},
	)
}

func getRandomID() int {
	/*// initialize the random number generator
	rand.Seed(time.Now().UnixNano())

	// generate a random number between 1 and 3
	return rand.Intn(3) + 1*/
	return 1
}
