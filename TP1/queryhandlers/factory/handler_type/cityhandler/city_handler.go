package cityhandler

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
	"tp1/communication"
	"tp1/domain/business/distanceaccumulator"
	"tp1/domain/business/queryresponse"
	"tp1/domain/entities/eof"
	"tp1/queryhandlers/factory/handler_type/cityhandler/config"
)

const (
	queryID         = "3"
	handlerType     = "city-handler"
	handlerStr      = "cityhandler"
	contentTypeJson = "application/json"
)

type CityHandler struct {
	rabbitMQ      *communication.RabbitMQ
	config        *config.CityHandlerConfig
	resultHandler map[string]*distanceaccumulator.DistanceAccumulator
}

func NewCityHandler(rabbitMQ *communication.RabbitMQ, cityHandlerConfig *config.CityHandlerConfig) *CityHandler {
	resultHandler := make(map[string]*distanceaccumulator.DistanceAccumulator)
	return &CityHandler{
		rabbitMQ:      rabbitMQ,
		config:        cityHandlerConfig,
		resultHandler: resultHandler,
	}
}

func (ch *CityHandler) getLogMessage(method string, message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[handler: %s][query: %s][method: %s][status: ERROR] %s: %s", handlerType, queryID, method, message, err.Error())
	}
	return fmt.Sprintf("[handler: %s][query: %s][method: %s][status: OK] %s", handlerType, queryID, method, message)
}

func (ch *CityHandler) GetQueryID() string {
	return queryID
}

func (ch *CityHandler) GetType() string {
	return handlerType
}

func (ch *CityHandler) GetEOFString() string {
	return fmt.Sprintf("eof.%s", handlerStr)
}

func (ch *CityHandler) GetExpectedEOFString() string {
	return fmt.Sprintf("eof.%s.%s", ch.config.PreviousStage, ch.config.City)
}

// DeclareQueues declares non-anonymous queues for City Handler
// Queues: EOF queue, City Handler queue, Response queue
func (ch *CityHandler) DeclareQueues() error {
	err := ch.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{
		//rh.config.EOFQueueConfig,
		ch.config.OutputQueue,
		ch.config.InputQueue,
	})
	if err != nil {
		return err
	}

	log.Info(ch.getLogMessage("DeclareQueues", "queues declared correctly!", nil))

	return nil
}

// GenerateResponse joins the data from all the previous stages. The flow of this function is:
// 1. Start consuming from the input queue
// 2. Merge each message with the previous one in the resultHandler of the CityHandler
// 3. Once an EOF message arrives, stop listening from the input queue
func (ch *CityHandler) GenerateResponse() error {
	inputQueueName := ch.config.InputQueue.Name
	consumer, err := ch.rabbitMQ.GetQueueConsumer(inputQueueName)
	if err != nil {
		log.Debug(ch.getLogMessage("GenerateResponse", "error getting consumer", err))
		return err
	}
	expectedEOF := ch.GetExpectedEOFString()

finishProcessing:
	for message := range consumer {
		var distanceAccumulators []*distanceaccumulator.DistanceAccumulator
		err = json.Unmarshal(message.Body, &distanceAccumulators)
		if err != nil {
			log.Error(ch.getLogMessage("GenerateResponse", "error unmarshalling data", err))
			return err
		}

		for idx := range distanceAccumulators {
			newDistanceAccumulator := distanceAccumulators[idx]

			metadata := newDistanceAccumulator.GetMetadata()

			if metadata.GetType() == ch.config.EOFType {
				// sanity check
				if metadata.GetMessage() != expectedEOF {
					panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", expectedEOF, metadata.GetMessage()))
				}

				log.Info(ch.getLogMessage("GenerateResponse", fmt.Sprintf("EOF received: %s", metadata.GetMessage()), nil))
				break finishProcessing
			}

			log.Debug(ch.getLogMessage("GenerateResponse", "received DistanceAccumulator", nil))

			accumulator, ok := ch.resultHandler[newDistanceAccumulator.StationID]
			if !ok {
				// we don't have data of the given distance accumulator
				ch.resultHandler[newDistanceAccumulator.StationID] = newDistanceAccumulator
				continue
			}
			// ToDo: check this licha
			accumulator.Merge(newDistanceAccumulator)
		}
	}

	log.Debug(ch.getLogMessage("GenerateResponse", "All data was joined successfully", nil))
	return nil
}

// SendResponse sends the response of the handler to the server
func (ch *CityHandler) SendResponse() error {
	var stationNames []string
	for _, accumulator := range ch.resultHandler {
		if accumulator.GetAverageDistance() >= ch.config.ThresholdDistance {
			stationNames = append(stationNames, accumulator.StationName)
		}
	}
	log.Debugf("LICHITA QUERY 3: %v", stationNames)

	response := fmt.Sprintf("Query %s result: %s", queryID, strings.Join(stationNames, "\n\t-"))
	queryResponse := queryresponse.NewQueryResponse(queryID, response, handlerType, "response")
	queryResponseBytes, err := json.Marshal(queryResponse)
	if err != nil {
		return fmt.Errorf("%w: error marshalling Query response message: %s", err, err.Error())
	}
	responseQueueName := ch.config.OutputQueue.Name

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.rabbitMQ.PublishMessageInQueue(ctx, responseQueueName, queryResponseBytes, contentTypeJson)
	if err != nil {
		log.Error(ch.getLogMessage("SendResponse", "error sending query response message", err))
		return err
	}
	return nil
}

// SendEOF notifies the EOF Manager that the work of this handler is done
func (ch *CityHandler) SendEOF() error {
	eofData := eof.NewEOF("", handlerType, ch.GetEOFString())
	eofDataBytes, err := json.Marshal(eofData)
	if err != nil {
		return fmt.Errorf("%w: error marshalling EOF message: %s", err, err.Error())
	}
	eofQueueName := ch.config.EOFQueueConfig.Name

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.rabbitMQ.PublishMessageInQueue(ctx, eofQueueName, eofDataBytes, contentTypeJson)
	if err != nil {
		log.Error(ch.getLogMessage("SendEOF", fmt.Sprintf("error sending EOF message: %s", ch.GetEOFString()), err))
		return err
	}
	return nil
}

func (ch *CityHandler) Kill() error {
	return ch.rabbitMQ.KillBadBunny()
}
