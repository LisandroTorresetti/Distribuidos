package rainhandler

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
	"tp1/communication"
	"tp1/domain/business/queryresponse"
	"tp1/domain/business/rainfallaccumulator"
	"tp1/domain/entities/eof"
	"tp1/queryhandlers/factory/handler_type/rainhandler/config"
)

const (
	queryID         = "1"
	handlerType     = "rain-handler"
	handlerStr      = "rainhandler"
	contentTypeJson = "application/json"
)

type RainHandler struct {
	rabbitMQ      *communication.RabbitMQ
	config        *config.RainHandlerConfig
	resultHandler *rainfallaccumulator.RainfallAccumulator
}

func NewRainHandler(rabbitMQ *communication.RabbitMQ, configRainHandler *config.RainHandlerConfig) *RainHandler {
	resultHandler := rainfallaccumulator.NewRainfallAccumulator()

	return &RainHandler{
		rabbitMQ:      rabbitMQ,
		config:        configRainHandler,
		resultHandler: resultHandler,
	}
}

func (rh *RainHandler) getLogMessage(method string, message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[handler: %s][query: %s][method: %s][status: ERROR] %s: %s", handlerType, queryID, method, message, err.Error())
	}
	return fmt.Sprintf("[handler: %s][query: %s][method: %s][status: OK] %s", handlerType, queryID, method, message)
}

func (rh *RainHandler) GetQueryID() string {
	return queryID
}

// GetType returns handler type
func (rh *RainHandler) GetType() string {
	return handlerType
}

// GetEOFString returns the Rain Handler EOF String.
func (rh *RainHandler) GetEOFString() string {
	return fmt.Sprintf("eof.%s", handlerStr)
}

// GetExpectedEOFString returns Rain Joiner's expected EOF
func (rh *RainHandler) GetExpectedEOFString() string {
	return fmt.Sprintf("eof.%s", rh.config.PreviousStage)
}

// DeclareQueues declares non-anonymous queues for Rain Handler
// Queues: EOF queue, Rain Handler queue, Response queue
func (rh *RainHandler) DeclareQueues() error {
	err := rh.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{
		//rh.config.EOFQueueConfig,
		rh.config.OutputQueue,
		rh.config.InputQueue,
	})
	if err != nil {
		return err
	}

	log.Info(rh.getLogMessage("DeclareQueues", "queues declared correctly!", nil))

	return nil
}

// GenerateResponse joins the data from all the previous stages. The flow of this function is:
// 1. Start consuming from the input queue
// 2. Merge each message with the previous one in the resultHandler of the RainHandler
// 3. Once an EOF message arrives, stop listening from the input queue
func (rh *RainHandler) GenerateResponse() error {
	inputQueueName := rh.config.InputQueue.Name
	consumer, err := rh.rabbitMQ.GetQueueConsumer(inputQueueName)
	if err != nil {
		log.Debug(rh.getLogMessage("GenerateResponse", "error getting consumer", err))
		return err
	}
	expectedEOF := rh.GetExpectedEOFString()

	for message := range consumer {
		var newRainfallAccumulator *rainfallaccumulator.RainfallAccumulator
		err = json.Unmarshal(message.Body, &newRainfallAccumulator)
		if err != nil {
			log.Error(rh.getLogMessage("GenerateResponse", "error unmarshalling data", err))
			return err
		}

		metadata := newRainfallAccumulator.GetMetadata()

		if metadata.GetType() == rh.config.EOFType {
			// sanity check
			if metadata.GetMessage() != expectedEOF {
				panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", expectedEOF, metadata.GetMessage()))
			}

			log.Info(rh.getLogMessage("GenerateResponse", fmt.Sprintf("EOF received: %s", metadata.GetMessage()), nil))
			break
		}

		log.Debug(rh.getLogMessage("GenerateResponse", "received RainfallAccumulator", nil))
		rh.resultHandler.Merge(newRainfallAccumulator)
	}

	log.Debug(rh.getLogMessage("GenerateResponse", "All data was joined successfully", nil))
	return nil
}

// SendResponse sends the response of the query to the server
func (rh *RainHandler) SendResponse() error {
	avgDuration := rh.resultHandler.GetAverageDuration()
	response := fmt.Sprintf("Query %s result: AVG duration %.4f", queryID, avgDuration)
	log.Debugf("LICHITA: %s", response)

	queryResponse := queryresponse.NewQueryResponse(queryID, response, handlerType, "response")
	queryResponseBytes, err := json.Marshal(queryResponse)
	if err != nil {
		return fmt.Errorf("%w: error marshalling Query response message: %s", err, err.Error())
	}
	responseQueueName := rh.config.OutputQueue.Name

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = rh.rabbitMQ.PublishMessageInQueue(ctx, responseQueueName, queryResponseBytes, contentTypeJson)
	if err != nil {
		log.Error(rh.getLogMessage("SendResponse", "error sending query response message", err))
		return err
	}
	return nil
}

// SendEOF notifies the EOF Manager that the work of this handler is done
func (rh *RainHandler) SendEOF() error {
	eofData := eof.NewEOF("", handlerType, rh.GetEOFString())
	eofDataBytes, err := json.Marshal(eofData)
	if err != nil {
		return fmt.Errorf("%w: error marshalling EOF message: %s", err, err.Error())
	}
	eofQueueName := rh.config.EOFQueueConfig.Name

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = rh.rabbitMQ.PublishMessageInQueue(ctx, eofQueueName, eofDataBytes, contentTypeJson)
	if err != nil {
		log.Error(rh.getLogMessage("SendEOF", fmt.Sprintf("error sending EOF message: %s", rh.GetEOFString()), err))
		return err
	}
	return nil
}

func (rh *RainHandler) Kill() error {
	return rh.rabbitMQ.KillBadBunny()
}
