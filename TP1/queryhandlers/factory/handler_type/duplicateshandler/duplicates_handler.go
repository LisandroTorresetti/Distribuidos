package duplicateshandler

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
	"tp1/communication"
	"tp1/domain/business/queryresponse"
	"tp1/domain/business/tripcounter"
	"tp1/domain/entities/eof"
	"tp1/queryhandlers/factory/handler_type/duplicateshandler/config"
)

const (
	queryID         = "2"
	handlerType     = "duplicates-handler"
	handlerStr      = "duplicateshandler"
	contentTypeJson = "application/json"
)

type DuplicatesHandler struct {
	rabbitMQ      *communication.RabbitMQ
	config        *config.DuplicatesHandlerConfig
	resultHandler map[int]*tripcounter.TripCounterCompound
}

func NewDuplicatesHandler(rabbitMQ *communication.RabbitMQ, duplicatesHandlerConfig *config.DuplicatesHandlerConfig) *DuplicatesHandler {
	resultHandler := make(map[int]*tripcounter.TripCounterCompound)
	return &DuplicatesHandler{
		rabbitMQ:      rabbitMQ,
		config:        duplicatesHandlerConfig,
		resultHandler: resultHandler,
	}
}

func (dh *DuplicatesHandler) getLogMessage(method string, message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[handler: %s][query: %s][method: %s][status: ERROR] %s: %s", handlerType, queryID, method, message, err.Error())
	}
	return fmt.Sprintf("[handler: %s][query: %s][method: %s][status: OK] %s", handlerType, queryID, method, message)
}

func (dh *DuplicatesHandler) GetQueryID() string {
	return queryID
}

func (dh *DuplicatesHandler) GetType() string {
	return handlerType
}

func (dh *DuplicatesHandler) GetEOFString() string {
	return fmt.Sprintf("eof.%s.%s", handlerStr, dh.config.City) // My eof message
}

func (dh *DuplicatesHandler) GetExpectedEOFString() string {
	return fmt.Sprintf("eof.%s.%s", dh.config.PreviousStage, dh.config.City)
}

func (dh *DuplicatesHandler) DeclareQueues() error {
	err := dh.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{
		dh.config.EOFQueueConfig,
		dh.config.OutputQueue,
		dh.config.InputQueue,
	})
	if err != nil {
		return err
	}

	log.Info(dh.getLogMessage("DeclareQueues", "queues declared correctly!", nil))

	return nil
}

func (dh *DuplicatesHandler) GenerateResponse() error {
	inputQueueName := dh.config.InputQueue.Name
	consumer, err := dh.rabbitMQ.GetQueueConsumer(inputQueueName)
	if err != nil {
		log.Debug(dh.getLogMessage("GenerateResponse", "error getting consumer", err))
		return err
	}
	expectedEOF := dh.GetExpectedEOFString()

finishProcessing:
	for message := range consumer {
		var tripCompoundAccumulators []*tripcounter.TripCounterCompound
		err = json.Unmarshal(message.Body, &tripCompoundAccumulators)
		if err != nil {
			log.Error(dh.getLogMessage("GenerateResponse", "error unmarshalling data", err))
			return err
		}

		for idx := range tripCompoundAccumulators {
			newTripCounterCompound := tripCompoundAccumulators[idx]

			metadata := newTripCounterCompound.Metadata

			if metadata.GetType() == dh.config.EOFType {
				// sanity check
				if metadata.GetMessage() != expectedEOF {
					panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", expectedEOF, metadata.GetMessage()))
				}

				log.Info(dh.getLogMessage("GenerateResponse", fmt.Sprintf("EOF received: %s", metadata.GetMessage()), nil))
				break finishProcessing
			}

			log.Debug(dh.getLogMessage("GenerateResponse", "received TripCounterCompound", nil))

			accumulator, ok := dh.resultHandler[newTripCounterCompound.StationID]
			if !ok {
				// we don't have data of the given distance accumulator
				dh.resultHandler[newTripCounterCompound.StationID] = newTripCounterCompound
				continue
			}
			// ToDo: check this licha
			accumulator.Merge(newTripCounterCompound)
		}
	}

	log.Debug(dh.getLogMessage("GenerateResponse", "All data was joined successfully", nil))
	return nil
}

func (dh *DuplicatesHandler) SendResponse() error {
	var stationNames []string

	for _, compoundCounter := range dh.resultHandler {
		if compoundCounter.DuplicateValues() {
			stationNames = append(stationNames, compoundCounter.StationName)
		}
	}

	log.Debugf("LICHITA QUERY 2: %v", stationNames)

	responseMessage := fmt.Sprintf("Query %s result: %s", queryID, strings.Join(stationNames, "\n\t-"))
	queryResponse := queryresponse.NewQueryResponse(queryID, responseMessage, handlerType, "response")
	queryResponseBytes, err := json.Marshal(queryResponse)
	if err != nil {
		return fmt.Errorf("%w: error marshalling Query response message: %s", err, err.Error())
	}
	responseQueueName := dh.config.OutputQueue.Name

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = dh.rabbitMQ.PublishMessageInQueue(ctx, responseQueueName, queryResponseBytes, contentTypeJson)
	if err != nil {
		log.Error(dh.getLogMessage("SendResponse", "error sending query response message", err))
		return err
	}
	return nil
}

// SendEOF notifies the EOF Manager that the work of this handler is done
func (dh *DuplicatesHandler) SendEOF() error {
	eofData := eof.NewEOF(dh.config.City, handlerType, dh.GetEOFString())
	eofDataBytes, err := json.Marshal(eofData)
	if err != nil {
		return fmt.Errorf("%w: error marshalling EOF message: %s", err, err.Error())
	}
	eofQueueName := dh.config.EOFQueueConfig.Name

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = dh.rabbitMQ.PublishMessageInQueue(ctx, eofQueueName, eofDataBytes, contentTypeJson)
	if err != nil {
		log.Error(dh.getLogMessage("SendEOF", fmt.Sprintf("error sending EOF message: %s", dh.GetEOFString()), err))
		return err
	}
	return nil
}

func (dh *DuplicatesHandler) Kill() error {
	return dh.rabbitMQ.KillBadBunny()
}
