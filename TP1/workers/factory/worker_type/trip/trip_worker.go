package trip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
	"tp1/communication"
	"tp1/domain/entities"
	"tp1/domain/entities/eof"
	"tp1/domain/entities/trip"
	"tp1/utils"
	dataErrors "tp1/workers/factory/worker_type/errors"
	"tp1/workers/factory/worker_type/trip/config"
)

const (
	dateLayout      = "2006-01-02"
	tripWorkerType  = "trips-worker"
	tripStr         = "trips"
	exchangeOutput  = "exchange_output_"
	outputTarget    = "output"
	contentTypeJson = "application/json"
)

type TripWorker struct {
	rabbitMQ *communication.RabbitMQ
	config   *config.TripWorkerConfig
}

func NewTripWorker(tripWorkerConfig *config.TripWorkerConfig, rabbitMQ *communication.RabbitMQ) *TripWorker {
	return &TripWorker{
		rabbitMQ: rabbitMQ,
		config:   tripWorkerConfig,
	}
}

// GetID returns the Weather Worker ID
func (tw *TripWorker) GetID() int {
	return tw.config.ID
}

// GetType returns the Weather Worker type
func (tw *TripWorker) GetType() string {
	return tripWorkerType
}

// GetRoutingKeys returns the Trip Worker routing keys: trips.city.workerID and eof.trips.city
func (tw *TripWorker) GetRoutingKeys() []string {
	return []string{
		fmt.Sprintf("%s.%s.%v", tripStr, tw.config.City, tw.GetID()), // input routing key: trips.city.workerID
		fmt.Sprintf("eof.%s.%s", tripStr, tw.config.City),            // eof.trips.city
	}
}

// GetEOFString returns the Trip Worker expected EOF String
func (tw *TripWorker) GetEOFString() string {
	return fmt.Sprintf("eof.%s.%s", tripStr, tw.config.City)
}

// DeclareQueues declares non-anonymous queues for Trip Worker
func (tw *TripWorker) DeclareQueues() error {
	err := tw.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{tw.config.EOFQueueConfig})
	if err != nil {
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] queues declared correctly!", tripStr, tw.GetID())
	return nil

}

// DeclareExchanges declares exchanges for Trip Worker
// Exchanges: trips-topic, trips-rainjoiner-topic, trips-yearjoiner-topic, trips-montrealjoiner-topic
func (tw *TripWorker) DeclareExchanges() error {
	var exchanges []communication.ExchangeDeclarationConfig
	for _, exchange := range tw.config.ExchangesConfig {
		exchanges = append(exchanges, exchange)
	}

	err := tw.rabbitMQ.DeclareExchanges(exchanges)
	if err != nil {
		return err
	}

	routingKeys := tw.GetRoutingKeys()
	err = tw.rabbitMQ.Bind([]string{tw.config.InputExchange}, routingKeys)
	if err != nil {
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] exchanges declared correctly!", tripStr, tw.GetID())
	return nil
}

// ProcessInputMessages process all messages that Trip Worker receives
func (tw *TripWorker) ProcessInputMessages() error {
	consumer, err := tw.rabbitMQ.GetConsumerForExchange(tw.config.InputExchange)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Infof("[worker: %s][workerID: %v][status: OK]start consuming messages", tripWorkerType, tw.GetID())
	eofString := tw.GetEOFString()

	for message := range consumer {
		msg := string(message.Body)
		if msg == eofString {
			log.Infof("[worker: %s][workerID: %v][status: OK] EOF received: %s", tripWorkerType, tw.GetID(), eofString)
			eofData := eof.NewEOF(tw.config.City, tripWorkerType, eofString)
			eofBytes, err := json.Marshal(eofData)
			if err != nil {
				log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error marshalling EOF message: %s", tripWorkerType, tw.GetID(), err.Error())
				return err
			}
			err = tw.rabbitMQ.PublishMessageInQueue(ctx, tw.config.EOFQueueConfig.Name, eofBytes, contentTypeJson)

			if err != nil {
				log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing EOF message: %s", tripWorkerType, tw.GetID(), err.Error())
				return err
			}
			break
		}

		log.Debugf("[worker: %s][workerID: %v][status: OK][method: ProcessInputMessages] received message %s", tripWorkerType, tw.GetID(), msg)
		err = tw.processData(ctx, msg)
		if err != nil {
			return err
		}
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] all data were processed", tripWorkerType, tw.GetID())
	return nil
}

func (tw *TripWorker) Kill() error {
	return tw.rabbitMQ.KillBadBunny()
}

// processData dataChunk is a string with the following format:
// trips,city,data1_1,data1_2,...,data1_N|trips,city,data2_1,data2_2...,data2_N|PING
// Only valid data from the received batch is sent to the next stage
func (tw *TripWorker) processData(ctx context.Context, dataChunk string) error {
	quartersMap, err := tw.getValidDataToSend(dataChunk)
	if err != nil {
		return err
	}

	if !hasDataToSend(quartersMap) {
		return nil
	}

	dataToSendByQuarter, err := tw.marshalDataToSend(quartersMap)
	if err != nil {
		return err
	}

	err = tw.publishDataInExchange(ctx, dataToSendByQuarter, tw.config.ExchangesConfig[exchangeOutput+"rain_joiner"].Name)
	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: Error] error publishing data in Rain Joiner", tripWorkerType, tw.GetID())
		return err
	}

	if utils.ContainsString(tw.config.City, tw.config.IncludeCities) {
		err = tw.publishDataInExchange(ctx, dataToSendByQuarter, tw.config.ExchangesConfig[exchangeOutput+"montreal_joiner"].Name)
		if err != nil {
			log.Errorf("[worker: %s][workerID: %v][status: Error] error publishing data in Montreal Joiner", tripWorkerType, tw.GetID())
			return err
		}
	}

	quartersMapTuned := tw.filterDataByYears(quartersMap)
	if !hasDataToSend(quartersMapTuned) {
		return nil
	}

	dataToSendByQuarterTuned, err := tw.marshalDataToSend(quartersMapTuned)
	if err != nil {
		return err
	}
	err = tw.publishDataInExchange(ctx, dataToSendByQuarterTuned, tw.config.ExchangesConfig[exchangeOutput+"year_joiner"].Name)
	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: Error] error publishing data in Year Joiner", tripWorkerType, tw.GetID())
		return err
	}

	return nil
}

func (tw *TripWorker) getTripData(data string) (*trip.TripData, error) {
	dataSplit := strings.Split(data, tw.config.DataFieldDelimiter)
	startDateStr := dataSplit[tw.config.ValidColumnsIndexes.StartDate]
	startDateStr = strings.Split(startDateStr, " ")[0] // To avoid hours:minutes:seconds
	startDate, err := time.Parse(dateLayout, startDateStr)
	if err != nil {
		log.Debugf("Invalid start date: %v", dataSplit[tw.config.ValidColumnsIndexes.StartDate])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidDate, dataErrors.ErrInvalidTripData)
	}

	endDateStr := dataSplit[tw.config.ValidColumnsIndexes.EndDate]
	endDateStr = strings.Split(endDateStr, " ")[0]
	endDate, err := time.Parse(dateLayout, endDateStr)
	if err != nil {
		log.Debugf("Invalid end date; %v", dataSplit[tw.config.ValidColumnsIndexes.EndDate])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidDate, dataErrors.ErrInvalidTripData)
	}

	startStationCodeID, err := strconv.Atoi(dataSplit[tw.config.ValidColumnsIndexes.StartStationCode])
	if err != nil {
		log.Debugf("Invalid start station code ID: %v", dataSplit[tw.config.ValidColumnsIndexes.StartStationCode])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrStationCodeType, dataErrors.ErrInvalidTripData)
	}

	endStationCodeID, err := strconv.Atoi(dataSplit[tw.config.ValidColumnsIndexes.EndStationCode])
	if err != nil {
		log.Debugf("Invalid end station code ID: %v", dataSplit[tw.config.ValidColumnsIndexes.EndStationCode])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrStationCodeType, dataErrors.ErrInvalidTripData)
	}

	yearID, err := strconv.Atoi(dataSplit[tw.config.ValidColumnsIndexes.YearID])
	if err != nil {
		log.Debugf("Invalid year ID: %v", dataSplit[tw.config.ValidColumnsIndexes.YearID])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidYearIDType, dataErrors.ErrInvalidTripData)
	}

	duration, err := strconv.ParseFloat(dataSplit[tw.config.ValidColumnsIndexes.Duration], 64)
	if err != nil {
		log.Debugf("Invalid duration type: %v", dataSplit[tw.config.ValidColumnsIndexes.Duration])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidDurationType, dataErrors.ErrInvalidTripData)
	}

	return &trip.TripData{
		StartDate:        startDate,
		StartStationCode: startStationCodeID,
		EndDate:          endDate,
		EndStationCode:   endStationCodeID,
		Duration:         duration,
		YearID:           yearID,
	}, nil
}

// getValidTripData returns a map organized by quarters (Q1, Q2, Q3, Q4) with valid data to send to the next stage.
func (tw *TripWorker) getValidDataToSend(dataChunk string) (map[string][]*trip.TripData, error) {
	dataSplit := strings.Split(dataChunk, tw.config.DataDelimiter)
	quartersMap := getQuartersMap()
	for _, data := range dataSplit {
		if strings.Contains(data, tw.config.EndBatchMarker) {
			log.Debug("bypassing PING")
			continue
		}

		tripData, err := tw.getTripData(data)
		if err != nil {
			if errors.Is(err, dataErrors.ErrInvalidTripData) {
				continue
			}
			return nil, err
		}

		if tw.isValid(tripData) {
			tripData.Metadata = entities.NewMetadata(tw.config.City, tripStr, tripWorkerType, "")
			quarterID := utils.GetQuarter(int(tripData.StartDate.Month()))
			quartersMap[quarterID] = append(quartersMap[quarterID], tripData)
		}
	}

	return quartersMap, nil
}

// isValid returns true if the following conditions are met:
// + The year of the StartDate must be equal to YearID value
// + The Duration of the trip is greater than 0
// + Both start station and end station must have an ID greater than 0
func (tw *TripWorker) isValid(tripData *trip.TripData) bool {
	validData := true
	var invalidReasons []string
	if tripData.StartDate.Year() != tripData.YearID {
		invalidReasons = append(invalidReasons, "StartDate year != YearID")
		validData = false
	}

	if tripData.Duration < 0.0 {
		invalidReasons = append(invalidReasons, "Trip duration < 0")
		validData = false
	}

	if tripData.StartStationCode < 0 {
		invalidReasons = append(invalidReasons, "StartStationCode < 0")
		validData = false
	}

	if tripData.EndStationCode < 0 {
		invalidReasons = append(invalidReasons, "EndStationCode < 0")
		validData = false
	}

	if !validData {
		log.Infof("[worker: %s][workerID: %v] Invalid data, reasons: %v", tripWorkerType, tw.GetID(), invalidReasons)
	}

	return validData
}

// marshalDataToSend returns a map with the quarters that have data to send
func (tw *TripWorker) marshalDataToSend(data map[string][]*trip.TripData) (map[string][]byte, error) {
	dataToSendMap := make(map[string][]byte)
	for key, value := range data {
		if len(value) > 0 {
			dataAsBytes, err := json.Marshal(value)
			if err != nil {
				log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error marshaling data: %s", tripWorkerType, tw.GetID(), err.Error())
				return nil, err
			}
			dataToSendMap[key] = dataAsBytes
		}
	}

	return dataToSendMap, nil
}

// publishDataInExchange publish the given chunk of data in exchangeName
func (tw *TripWorker) publishDataInExchange(ctx context.Context, dataToSendMap map[string][]byte, exchangeName string) error {
	targetStage := utils.GetTargetStage(exchangeName)
	for quarterID, dataToSend := range dataToSendMap {
		routingKey := fmt.Sprintf("%s.%s.%s", targetStage, tw.config.City, quarterID)
		err := tw.rabbitMQ.PublishMessageInExchange(ctx, exchangeName, routingKey, dataToSend, contentTypeJson)

		if err != nil {
			log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing message in join exchange: %s", tripWorkerType, tw.GetID(), err.Error())
			return err
		}
	}
	return nil
}

// filterDataByYears filters data, in each quarter, that has a year that it's not part of the valid ones
func (tw *TripWorker) filterDataByYears(data map[string][]*trip.TripData) map[string][]*trip.TripData {
	resultMap := getQuartersMap()
	for quarterID, quarterData := range data {
		for idx := range quarterData {
			actualData := *quarterData[idx]
			if utils.ContainsInt(actualData.YearID, tw.config.IncludeYears) {
				resultMap[quarterID] = append(resultMap[quarterID], &actualData) // create a new pointer to Trip Data
			}
		}
	}
	return resultMap
}

func getQuartersMap() map[string][]*trip.TripData {
	quartersMap := make(map[string][]*trip.TripData)
	quartersMap["Q1"] = []*trip.TripData{}
	quartersMap["Q2"] = []*trip.TripData{}
	quartersMap["Q3"] = []*trip.TripData{}
	quartersMap["Q4"] = []*trip.TripData{}
	return quartersMap
}

func hasDataToSend(data map[string][]*trip.TripData) bool {
	for _, value := range data {
		if len(value) > 0 {
			return true
		}
	}
	return false
}
