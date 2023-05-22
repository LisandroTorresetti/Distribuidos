package distancecalculator

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/umahmood/haversine"
	"time"
	"tp1/communication"
	"tp1/domain/business/distanceaccumulator"
	"tp1/domain/entities"
	"tp1/domain/entities/eof"
	"tp1/domain/entities/station"
	"tp1/utils"
	"tp1/workers/factory/worker_type/distancecalculator/config"
)

const (
	workerType                  = "distance-calculator"
	distanceCalculatorStr       = "distancecalculator"
	outputTarget                = "output"
	contentTypeJson             = "application/json"
	distanceAccumulatorDataType = "distance-accumulator"
)

type DistanceCalculator struct {
	rabbitMQ       *communication.RabbitMQ
	config         *config.DistanceCalculatorConfig
	distancesCache map[string]float64
}

func NewDistanceCalculator(distanceCalculatorConfig *config.DistanceCalculatorConfig, rabbitMQ *communication.RabbitMQ) *DistanceCalculator {
	distancesCache := make(map[string]float64)
	return &DistanceCalculator{
		rabbitMQ:       rabbitMQ,
		config:         distanceCalculatorConfig,
		distancesCache: distancesCache,
	}
}

func (dc *DistanceCalculator) getLogMessage(method string, message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[worker: %s][workerID: %v][method: %s][status: ERROR] %s: %s", workerType, dc.GetID(), method, message, err.Error())
	}
	return fmt.Sprintf("[worker: %s][workerID: %v][method: %s][status: OK] %s", workerType, dc.GetID(), method, message)
}

func (dc *DistanceCalculator) GetID() int {
	return dc.config.ID
}

func (dc *DistanceCalculator) GetType() string {
	return workerType
}

func (dc *DistanceCalculator) GetRoutingKeys() []string {
	panic("not needed")
}

// GetEOFString returns the Station Worker expected EOF String
func (dc *DistanceCalculator) GetEOFString() string {
	return fmt.Sprintf("eof.%s.%s", dc.config.PreviousStage, dc.config.City)
}

// DeclareQueues declares non-anonymous queues for Distance Calculator
func (dc *DistanceCalculator) DeclareQueues() error {
	queueConfigs := []communication.QueueDeclarationConfig{
		dc.config.InputQueueConfig,
		dc.config.EOFQueueConfig,
	}

	err := dc.rabbitMQ.DeclareNonAnonymousQueues(queueConfigs)
	if err != nil {
		return err
	}

	log.Info(dc.getLogMessage("DeclareQueues", "queues declared correctly!", nil))

	/*err = dc.rabbitMQ.SetQualityOfService(dc.config.QualityOfService)
	if err != nil {
		log.Error(dc.getLogMessage("DeclareQueues", "error setting quality of service", err))
		return err
	}*/
	return nil
}

// DeclareExchanges declares exchanges for Distance Calculator
func (dc *DistanceCalculator) DeclareExchanges() error {
	err := dc.rabbitMQ.DeclareExchanges([]communication.ExchangeDeclarationConfig{dc.config.OutputExchange})
	if err != nil {
		return err
	}

	log.Info(dc.getLogMessage("DeclareExchanges", "exchanges declared correctly!", nil))
	return nil
}

// ProcessInputMessages receives data from City Joiner, calculates the distance between two stations
// and then send a batch of DistanceAccumulators to the City Handler.
// When an EOF is received, is sent to the EOF manager
func (dc *DistanceCalculator) ProcessInputMessages() error {
	inputQueueName := dc.config.InputQueueConfig.Name
	consumer, err := dc.rabbitMQ.GetQueueConsumer(inputQueueName)
	if err != nil {
		log.Debug(dc.getLogMessage("ProcessInputMessages", "error getting consumer", err))
		return err
	}

	expectedEOF := dc.GetEOFString()

finishProcessing:
	for message := range consumer {
		log.Debugf("MENSAJE RECIBIDO: %s", string(message.Body))
		var multipleStationsData []*station.MultipleStations
		err = json.Unmarshal(message.Body, &multipleStationsData)
		if err != nil {
			log.Error(dc.getLogMessage("ProcessInputMessages", "error unmarshalling data", err))
			return err
		}

		var distanceAccumulators []*distanceaccumulator.DistanceAccumulator
		workerMetadata := entities.NewMetadata(
			dc.config.City,
			distanceAccumulatorDataType,
			workerType,
			"",
		)
		for idx := range multipleStationsData {
			multipleStation := multipleStationsData[idx]

			metadata := multipleStation.GetMetadata()

			if metadata.GetType() == dc.config.EOFType {
				// sanity check
				if metadata.GetMessage() != expectedEOF {
					panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", expectedEOF, metadata.GetMessage()))
				}

				log.Info(dc.getLogMessage("ProcessInputMessages", fmt.Sprintf("EOF received: %s", metadata.GetMessage()), nil))
				break finishProcessing
			}

			log.Debug(dc.getLogMessage("ProcessInputMessages", "received MultipleStation data. Calculating distance...", nil))

			mapKey := multipleStation.GetCombinedIDs()
			distance, ok := dc.distancesCache[mapKey]
			if !ok {
				log.Debug(dc.getLogMessage("ProcessInputMessages", "distance not found in cache", nil))
				latStartStation, longStartStation := multipleStation.GetStartStationCoordinates()
				latEndStation, longEndStation := multipleStation.GetEndStationCoordinates()
				distance = calculateDistance(latStartStation, longStartStation, latEndStation, longEndStation)
			}

			distanceAccumulator := distanceaccumulator.NewDistanceAccumulatorWithData(
				workerMetadata,
				multipleStation.GetEndStationName(),
				fmt.Sprintf("%v", multipleStation.EndStationID),
				1,
				distance,
			)
			distanceAccumulators = append(distanceAccumulators, distanceAccumulator)

			_, ok = dc.distancesCache[mapKey]
			if !ok {
				// Update distance cache
				log.Debug(dc.getLogMessage("ProcessInputMessages", "Add distance to cache", nil))
				dc.distancesCache[mapKey] = distance
			}
		}

		err = dc.sendBatch(distanceAccumulators)
		if err != nil {
			return err
		}
	}

	err = dc.sendEOF()
	if err != nil {
		return err
	}

	_ = dc.rabbitMQ.KillBadBunny()

	log.Info(dc.getLogMessage("ProcessInputMessages", "All distances were calculated correctly!", nil))
	return nil
}

// sendBatch sends multiple batches to City Handler. The data is grouped by the target ID in multiple slices
func (dc *DistanceCalculator) sendBatch(batchDistanceAccumulators []*distanceaccumulator.DistanceAccumulator) error {
	if len(batchDistanceAccumulators) <= 0 {
		log.Debug(dc.getLogMessage("sendBatch", "nothing to send", nil))
		return nil
	}

	targetMap := getTargetMap()
	for idx := range batchDistanceAccumulators {
		distanceAcc := batchDistanceAccumulators[idx]
		targetID := dc.getTargetID(distanceAcc.GetIDAsInt())
		targetMap[targetID] = append(targetMap[targetID], distanceAcc)
	}

	dataMarshalled, err := dc.marshalDataToSend(targetMap)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exchangeName := dc.config.OutputExchange.Name
	targetStage := utils.GetTargetStage(exchangeName) // ToDo: we could use the value from the config directly (Next stage)
	for id, dataToSend := range dataMarshalled {
		routingKey := fmt.Sprintf("%s.%s.%s", targetStage, dc.config.City, id)
		log.Debug(dc.getLogMessage("sendBatch", fmt.Sprintf("sending batch to exchange %s with routing key %s", exchangeName, routingKey), nil))
		err = dc.rabbitMQ.PublishMessageInExchange(ctx, exchangeName, routingKey, dataToSend, contentTypeJson)
		if err != nil {
			log.Error(dc.getLogMessage("sendBatch", fmt.Sprintf("error publishing in exchange %s", exchangeName), err))
			return err
		}
	}
	return nil
}

func (dc *DistanceCalculator) sendEOF() error {
	eofMessage := fmt.Sprintf("eof.%s.%s", distanceCalculatorStr, dc.config.City)
	eofData := eof.NewEOF(dc.config.City, workerType, eofMessage)
	eofDataBytes, err := json.Marshal(eofData)
	if err != nil {
		return fmt.Errorf("%w: error marshalling EOF message: %s", err, err.Error())
	}
	eofQueueName := dc.config.EOFQueueConfig.Name

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = dc.rabbitMQ.PublishMessageInQueue(ctx, eofQueueName, eofDataBytes, contentTypeJson)
	if err != nil {
		log.Error(dc.getLogMessage("SendEOF", fmt.Sprintf("error sending EOF message: %s", eofMessage), err))
		return err
	}
	return nil
}

func (dc *DistanceCalculator) Kill() error {
	return dc.rabbitMQ.KillBadBunny()
}

func (dc *DistanceCalculator) getTargetID(stationID int) string {
	id := (stationID % dc.config.AmountOfPartitions) + 1
	return fmt.Sprintf("%v", id)
}

// marshalDataToSend returns a map with the quarters that have data to send
func (dc *DistanceCalculator) marshalDataToSend(data map[string][]*distanceaccumulator.DistanceAccumulator) (map[string][]byte, error) {
	dataToSendMap := make(map[string][]byte)
	for key, value := range data {
		if len(value) > 0 {
			dataAsBytes, err := json.Marshal(value)
			if err != nil {
				log.Error(dc.getLogMessage("marshalDataToSend", "error marshaling data", err))
				return nil, err
			}
			dataToSendMap[key] = dataAsBytes
		}
	}

	return dataToSendMap, nil
}

// calculateDistance returns the distance between two stations using haversine formula
func calculateDistance(latStartStation float64, longStartStation float64, latEndStation float64, longEndStation float64) float64 {
	station1 := haversine.Coord{Lat: latStartStation, Lon: longStartStation}
	station2 := haversine.Coord{Lat: latEndStation, Lon: longEndStation}

	_, km := haversine.Distance(station1, station2)
	return km
}

func getTargetMap() map[string][]*distanceaccumulator.DistanceAccumulator {
	targetMap := make(map[string][]*distanceaccumulator.DistanceAccumulator)
	targetMap["1"] = []*distanceaccumulator.DistanceAccumulator{}
	targetMap["2"] = []*distanceaccumulator.DistanceAccumulator{}
	targetMap["3"] = []*distanceaccumulator.DistanceAccumulator{}
	targetMap["4"] = []*distanceaccumulator.DistanceAccumulator{}

	return targetMap
}
