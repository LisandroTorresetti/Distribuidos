package handler

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
	"time"
	"tp1/communication"
	"tp1/socket"
	"tp1/utils"
)

type MessageHandlerConfig struct {
	EndBatchMarker string
	FinMessages    []string
	AckMessage     string
}

type MessageHandler struct {
	config        MessageHandlerConfig
	handlerSocket *socket.Socket
	rabbitMQ      *communication.RabbitMQ
	actualCity    *string
}

func NewMessageHandler(config MessageHandlerConfig, handlerSocket *socket.Socket, rabbitMQ *communication.RabbitMQ) *MessageHandler {
	return &MessageHandler{
		config:        config,
		handlerSocket: handlerSocket,
		rabbitMQ:      rabbitMQ,
		actualCity:    nil,
	}
}

func (mh *MessageHandler) ProcessData() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
				log.Errorf("[server] error sending ACK to FIN message %s: %s", message, err.Error())
				return err
			}
			log.Debug("[server] Fin message ACK sent correctly")

			dataType := strings.SplitN(message, "-", 2)[0]
			err = mh.redlineEOF(ctx, dataType)
			if err != nil {
				return err
			}
			break
		}

		if strings.Contains(message, mh.config.EndBatchMarker) {
			err = mh.handleDataMessage(ctx, message)
			if err != nil {
				log.Errorf("[server] error publishing message %s: %s", message, err.Error())
				return err
			}
		}
	}

	return nil
}

func (mh *MessageHandler) handleDataMessage(ctx context.Context, message string) error {
	err := mh.handlerSocket.Send(mh.config.AckMessage)
	if err != nil {
		log.Errorf("[server] error sending ACK to client: %s", err.Error())
		return err
	}

	dataType, city := getDataTypeAndCityFromMessage(message)
	if mh.actualCity == nil {
		mh.actualCity = &city
	} else if city != *mh.actualCity {
		// we've received all the data form some city, we can send an eof to that node
		log.Debugf("[server] actual city has changed: Old: %s - New: %s", *mh.actualCity, city)
		err = mh.redlineEOF(ctx, dataType)
		if err != nil {
			return err
		}
		mh.actualCity = &city
	}

	targetExchange := getTargetExchange(dataType)
	randomID := getRandomID()
	routingKey := fmt.Sprintf("%s.%s.%v", dataType, city, randomID) // routingKey could be weather.city.id, trips.city.id or stations.city.id
	err = mh.rabbitMQ.PublishMessageInExchange(ctx, targetExchange, routingKey, []byte(message), "text/plain")
	if err != nil {
		log.Errorf("[server] error publishing message in exchange %s with routing key %s", targetExchange, routingKey)
		return err
	}
	return nil
}

// redlineEOF sends an eof message due to all the data related with some stage was processed
func (mh *MessageHandler) redlineEOF(ctx context.Context, dataType string) error {
	targetExchange := getTargetExchange(dataType)
	routingKey := fmt.Sprintf("eof.%s.%s", dataType, *mh.actualCity) // eof.dataType.city
	message := routingKey                                            // both are equal

	err := mh.rabbitMQ.PublishMessageInExchange(ctx, targetExchange, routingKey, []byte(message), "text/plain")
	if err != nil {
		log.Errorf("[server] error publishing eof message in exchange %s with routing key %s", targetExchange, routingKey)
		return err
	}
	return nil
}

// getDataTypeAndCityFromMessage returns the dataType (weather, trips, stations) and city (montreal, toronto, washington) from the message
func getDataTypeAndCityFromMessage(message string) (string, string) {
	regex := regexp.MustCompile(`^([^,]+),([^,]+).*`)
	matches := regex.FindStringSubmatch(message)
	if len(matches) == 3 {
		return matches[1], matches[2]
	}
	panic("[server] data type or city are missing")
}

func getRandomID() int {
	/*// initialize the random number generator
	rand.Seed(time.Now().UnixNano())

	// generate a random number between 1 and 3
	return rand.Intn(3) + 1*/
	return 1
}

func getTargetExchange(dataType string) string {
	return fmt.Sprintf("%s-topic", dataType)
}
