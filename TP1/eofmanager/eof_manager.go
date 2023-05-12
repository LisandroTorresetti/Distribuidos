package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"regexp"
	"time"
	"tp1/communication"
)

const eofQueue = "eof-queue"

type eofConfig struct {
	Counters  map[string]map[string]int `yaml:"counters"`
	Queues    []string                  `yaml:"queues"`
	Exchanges []string                  `yaml:"exchanges"`
	Responses map[string][]string       `yaml:"responses"`
}

type EOFManager struct {
	config   *eofConfig
	rabbitMQ *communication.RabbitMQ
}

type EOFMessageComponents struct {
	Stage     string
	City      string
	ExtraData string
}

func getEOFComponents(eofMessage string) EOFMessageComponents {
	regex := regexp.MustCompile(`^eof\.([^.]+)\.([^.]+)(.*)`)
	matches := regex.FindStringSubmatch(eofMessage)

	if len(matches) < 3 {
		panic(fmt.Sprintf("[EOF Manager] invalid eof message %s", eofMessage))
	}

	extraData := ""
	if len(matches) == 4 {
		extraData = matches[3]
	}

	return EOFMessageComponents{
		Stage:     matches[1],
		City:      matches[2],
		ExtraData: extraData,
	}
}

func NewEOFManager(config *eofConfig, rabbitMQ *communication.RabbitMQ) *EOFManager {
	return &EOFManager{
		config:   config,
		rabbitMQ: rabbitMQ,
	}
}

func (eof *EOFManager) DeclareQueues() error {
	queueDeclarationConfig := communication.QueueDeclarationConfig{
		Name:             eofQueue,
		Durable:          true,
		DeleteWhenUnused: false,
		Exclusive:        true,
		NoWait:           false,
	}

	err := eof.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{queueDeclarationConfig})
	if err != nil {
		return err
	}
	log.Info("[EOF Manager] all queues were declared correctly")
	return nil
}

func (eof *EOFManager) DeclareExchanges() error {
	var exchangesDeclarations []communication.ExchangeDeclarationConfig
	for _, exchangeName := range eof.config.Exchanges {
		exchangeDeclaration := communication.ExchangeDeclarationConfig{
			Name:        exchangeName,
			Type:        "topic",
			Durable:     true,
			AutoDeleted: false,
			Internal:    false,
			NoWait:      false,
		}
		exchangesDeclarations = append(exchangesDeclarations, exchangeDeclaration)
	}

	err := eof.rabbitMQ.DeclareExchanges(exchangesDeclarations)
	if err != nil {
		return err
	}
	log.Info("[eof manager] all exchanges were declared correctly")
	return nil
}

func (eof *EOFManager) StartManaging() {
	infinite := make(chan error, 1)

	// weather filter handler
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		consumer, err := eof.rabbitMQ.GetQueueConsumer("eof-queue")
		if err != nil {
			infinite <- err
		}

		log.Debugf("[EOF Manager] start consuming messages")
		for message := range consumer {
			eofMessage := string(message.Body)
			eofComponents := getEOFComponents(eofMessage)

			actualValue, ok := eof.config.Counters[eofComponents.Stage][eofComponents.City]
			if !ok {
				panic(fmt.Sprintf("[EOF Manager] cannot found pair %s-%s", eofComponents.Stage, eofComponents.City))
			}

			updatedValue := actualValue - 1
			if updatedValue < 0 {
				panic(fmt.Sprintf("[EOF Manager] received more EOF than expected for pair %s-%s", eofComponents.Stage, eofComponents.City))
			}

			eof.config.Counters[eofComponents.Stage][eofComponents.City] = updatedValue
			if updatedValue == 0 {
				err = eof.sendStartProcessingMessage(ctx, eofComponents)
				if err != nil {
					panic(err.Error())
				}
			}

		}
	}()

	err := <-infinite
	log.Errorf("[EOF Manager] got some error handling EOFs: %s", err.Error())
}

func (eof *EOFManager) sendStartProcessingMessage(ctx context.Context, eofComponents EOFMessageComponents) error {
	targetExchanges, ok := eof.config.Responses[eofComponents.Stage]
	if !ok {
		panic(fmt.Sprintf("[EOF Manager] cannot found exchange to send response for key %s", eofComponents.Stage))
	}
	routingKey := fmt.Sprintf("eof.%s.%s", eofComponents.Stage, eofComponents.City) // ToDo: potential bug. Licha
	message := []byte(routingKey)
	for _, exchange := range targetExchanges {
		err := eof.rabbitMQ.PublishMessageInExchange(ctx, exchange, routingKey, message, "text/plain")
		if err != nil {
			log.Errorf("[EOF Manager] error publishing message in exchange %s", exchange)
			return err
		}
	}
	log.Infof("[EOF Manager] message sent correctly to exchanges: %v", targetExchanges)
	return nil
}
