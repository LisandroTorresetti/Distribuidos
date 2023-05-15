package main

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"regexp"
	"time"
	"tp1/communication"
	"tp1/domain/entities"
	eofEntity "tp1/domain/entities/eof"
	"tp1/utils"
)

const (
	eofQueue        = "eof-queue"
	contentTypeJson = "application/json"
	eofManagerStr   = "EOF-Manager"
)

type eofConfig struct {
	EOFType   string                    `yaml:"eof_type"`
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

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		consumer, err := eof.rabbitMQ.GetQueueConsumer("eof-queue")
		if err != nil {
			infinite <- err
		}

		log.Debugf("[EOF Manager] start consuming messages")
		for message := range consumer {
			var eofData eofEntity.EOFData
			err = json.Unmarshal(message.Body, &eofData)
			if err != nil {
				panic(fmt.Sprintf("[EOF Manager] error unmarshalling EOF Data: %s", err.Error()))
			}
			eofMetadata := eofData.GetMetadata()

			// sanity-check
			if eofMetadata.GetType() != eof.config.EOFType {
				panic(fmt.Sprintf("received an invalid type. Expected: %s - Got: %s", eof.config.EOFType, eofMetadata.GetType()))
			}

			stage := eofMetadata.GetStage()
			city := eofMetadata.GetCity()

			actualValue, ok := eof.config.Counters[stage][city]
			if !ok {
				panic(fmt.Sprintf("[EOF Manager] cannot found pair %s-%s", stage, city))
			}

			updatedValue := actualValue - 1
			log.Debugf("UPDATED VALUE %s: %v", eofMetadata.GetMessage(), updatedValue)
			if updatedValue < 0 {
				panic(fmt.Sprintf("[EOF Manager] received more EOF than expected for pair %s-%s", stage, city))
			}

			eof.config.Counters[stage][city] = updatedValue
			if updatedValue == 0 {
				err = eof.sendStartProcessingMessage(ctx, eofMetadata)
				if err != nil {
					panic(err.Error())
				}
			}
		}
	}()

	err := <-infinite
	log.Errorf("[EOF Manager] got some error handling EOFs: %s", err.Error())
}

func (eof *EOFManager) sendStartProcessingMessage(ctx context.Context, eofMetadata entities.Metadata) error {
	targetExchanges, ok := eof.config.Responses[eofMetadata.GetStage()]
	if !ok {
		panic(fmt.Sprintf("[EOF Manager] cannot found exchange to send response for key %s", eofMetadata.GetStage()))
	}

	for _, exchange := range targetExchanges {
		targetStage := utils.GetTargetStage(exchange)                              // from the exchange name we get the target (stage N) to send the EOF of stage N - 1
		routingKey := fmt.Sprintf("eof.%s.%s", targetStage, eofMetadata.GetCity()) // where I want to send the EOF message, eg: eof.rainjoiner.city

		eofMessage := eofEntity.NewEOF(eofMetadata.GetCity(), eofManagerStr, eofMetadata.GetMessage()) // eg message: eof.weather.montreal
		eofMessageBytes, err := json.Marshal([]*eofEntity.EOFData{eofMessage})
		if err != nil {
			panic(fmt.Sprintf("[EOF Manager] error marshalling EOF message to send, routing key %s: %s", routingKey, err.Error()))
		}

		err = eof.rabbitMQ.PublishMessageInExchange(ctx, exchange, routingKey, eofMessageBytes, contentTypeJson)
		if err != nil {
			log.Errorf("[EOF Manager] error publishing message in exchange %s", exchange)
			return err
		}
	}
	log.Infof("[EOF Manager] message sent correctly to exchanges: %v", targetExchanges)
	return nil
}
