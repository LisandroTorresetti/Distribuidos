package main

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
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
	topicStr        = "topic"
	pong            = "PONG"
	wildcardCity    = "nn"
)

type eofConfig struct {
	EOFType       string                               `yaml:"eof_type"`
	Counters      map[string]map[string]int            `yaml:"counters"`
	Queues        []string                             `yaml:"queues"`
	Exchanges     []string                             `yaml:"exchanges"`
	Responses     map[string][]string                  `yaml:"responses"`
	SpecialCases  []string                             `yaml:"special_cases"`
	QueryHandlers []string                             `yaml:"query_handlers"`
	InputQueue    communication.QueueDeclarationConfig `yaml:"input_queue"`
}

type EOFManager struct {
	config   *eofConfig
	rabbitMQ *communication.RabbitMQ
}

func NewEOFManager(config *eofConfig, rabbitMQ *communication.RabbitMQ) *EOFManager {
	return &EOFManager{
		config:   config,
		rabbitMQ: rabbitMQ,
	}
}

func (eof *EOFManager) DeclareQueues() error {
	err := eof.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{eof.config.InputQueue})
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

			notify := false
			if utils.ContainsString(stage, eof.config.SpecialCases) {
				notify = eof.specialCaseEnds(stage)
			} else if utils.ContainsString(stage, eof.config.QueryHandlers) {
				notify = eof.allQueriesProcessed()
			} else {
				notify = updatedValue == 0
			}

			if notify {
				err = eof.sendEOFToNextStage(ctx, eofMetadata)
				if err != nil {
					panic(err.Error())
				}
			}
		}
	}()

	err := <-infinite
	log.Errorf("[EOF Manager] got some error handling EOFs: %s", err.Error())
}

// sendEOFToNextStage sends an eof to the next stages of the actual one
func (eof *EOFManager) sendEOFToNextStage(ctx context.Context, eofMetadata entities.Metadata) error {
	if eof.publishInExchange(eofMetadata.GetStage()) {
		return eof.handlePublishInExchange(ctx, eofMetadata)
	}

	return eof.handlePublishInQueue(ctx, eofMetadata)
}

func (eof *EOFManager) handlePublishInExchange(ctx context.Context, eofMetadata entities.Metadata) error {
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

func (eof *EOFManager) handlePublishInQueue(ctx context.Context, eofMetadata entities.Metadata) error {
	targetQueues, ok := eof.config.Responses[eofMetadata.GetStage()]
	if !ok {
		panic(fmt.Sprintf("[EOF Manager] cannot found queue to send response for key %s", eofMetadata.GetStage()))
	}

	eofMessage := eofMetadata.GetMessage()
	if utils.ContainsString(eofMetadata.GetStage(), eof.config.SpecialCases) {
		regex := regexp.MustCompile(`(eof\.[^.]+).*`) // replace the original city by the wildcard
		match := regex.FindStringSubmatch(eofMetadata.GetMessage())
		eofMessage = fmt.Sprintf("%s.%s", match[1], wildcardCity)
	} else if utils.ContainsString(eofMetadata.GetStage(), eof.config.QueryHandlers) {
		eofMessage = pong
	}

	eofToSend := eofEntity.NewEOF(eofMetadata.GetCity(), eofManagerStr, eofMessage) // eg message: eof.weather.montreal
	eofToSendBytes, err := json.Marshal([]*eofEntity.EOFData{eofToSend})
	if err != nil {
		panic(fmt.Sprintf("[EOF Manager] error marshalling EOF message to send: %s", err.Error()))
	}

	targetQueueName := targetQueues[0]
	formatName := !utils.ContainsString(eofMetadata.GetStage(), eof.config.SpecialCases) &&
		!utils.ContainsString(eofMetadata.GetStage(), eof.config.QueryHandlers)
	if formatName {
		log.Debugf("LICHITA VOY A FORMATEAR QUEUE, %s", targetQueueName)
		// we have to parse it
		targetQueueName = fmt.Sprintf(targetQueueName, eofMetadata.GetCity())
	}
	log.Debugf("LICHITA TARGET QUEUE: %s", targetQueueName)

	err = eof.rabbitMQ.PublishMessageInQueue(ctx, targetQueueName, eofToSendBytes, contentTypeJson)
	if err != nil {
		panic(fmt.Sprintf("[EOF Manager] error publishing message in queue %s", targetQueueName))
	}
	return nil
}

// specialCaseEnds returns true if all counters of specialCase are in zero, otherwise false
func (eof *EOFManager) specialCaseEnds(specialCase string) bool {
	for _, counter := range eof.config.Counters[specialCase] {
		if counter != 0 {
			return false
		}
	}
	return true
}

// allQueriesProcessed returns true if the counters of each query handler is in zero, otherwise false
func (eof *EOFManager) allQueriesProcessed() bool {
	for _, queryHandler := range eof.config.QueryHandlers {
		for _, counter := range eof.config.Counters[queryHandler] {
			if counter != 0 {
				return false
			}
		}
	}
	return true
}

// publishInExchange returns true if the stage needs to publish in an exchange, otherwise false
func (eof *EOFManager) publishInExchange(stage string) bool {
	targetExchanges, ok := eof.config.Responses[stage]
	if !ok {
		panic(fmt.Sprintf("[EOF Manager] cannot found exchange to send response for key %s", stage))
	}

	return strings.Contains(targetExchanges[0], topicStr)
}
