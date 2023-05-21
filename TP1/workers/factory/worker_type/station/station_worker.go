package station

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
	"tp1/domain/entities/station"
	"tp1/utils"
	dataErrors "tp1/workers/factory/worker_type/errors"
	"tp1/workers/factory/worker_type/station/config"
)

const (
	stationWorkerType = "stations-worker"
	stationStr        = "stations"
	exchangeOutput    = "exchange_output_"
	contentTypeJson   = "application/json"
)

type StationWorker struct {
	rabbitMQ *communication.RabbitMQ
	config   *config.StationWorkerConfig
}

func NewStationWorker(stationWorkerConfig *config.StationWorkerConfig, rabbitMQ *communication.RabbitMQ) *StationWorker {
	return &StationWorker{
		rabbitMQ: rabbitMQ,
		config:   stationWorkerConfig,
	}
}

// GetID returns the Station Worker ID
func (sw *StationWorker) GetID() int {
	return sw.config.ID
}

// GetType returns the Station Worker type
func (sw *StationWorker) GetType() string {
	return stationWorkerType
}

// GetRoutingKeys returns the Station Worker routing keys
func (sw *StationWorker) GetRoutingKeys() []string {
	return []string{
		fmt.Sprintf("%s.%s.%v", stationStr, sw.config.City, sw.GetID()), // input routing key: stations.city.workerID
		fmt.Sprintf("eof.%s.%s", stationStr, sw.config.City),            // eof.stations.city
	}
}

// GetEOFString returns the Station Worker expected EOF String
func (sw *StationWorker) GetEOFString() string {
	return fmt.Sprintf("eof.%s.%s", stationStr, sw.config.City)
}

// DeclareQueues declares non-anonymous queues for Station Worker
func (sw *StationWorker) DeclareQueues() error {
	err := sw.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{sw.config.EOFQueueConfig})
	if err != nil {
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] queues declared correctly!", stationStr, sw.GetID())
	return nil

}

// DeclareExchanges declares exchanges for Station Worker
// Exchanges: stations-topic, stations-yearjoiner-topic, stations-cityjoiner-topic
func (sw *StationWorker) DeclareExchanges() error {
	var exchanges []communication.ExchangeDeclarationConfig
	for _, exchange := range sw.config.ExchangesConfig {
		exchanges = append(exchanges, exchange)
	}

	err := sw.rabbitMQ.DeclareExchanges(exchanges)
	if err != nil {
		return err
	}

	routingKeys := sw.GetRoutingKeys()
	err = sw.rabbitMQ.Bind([]string{sw.config.InputExchange}, routingKeys)
	if err != nil {
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] exchanges declared correctly!", stationStr, sw.GetID())
	return nil
}

// ProcessInputMessages process all messages that Station Worker receives
func (sw *StationWorker) ProcessInputMessages() error {
	consumer, err := sw.rabbitMQ.GetConsumerForExchange(sw.config.InputExchange)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Infof("[worker: %s][workerID: %v][status: OK]start consuming messages", stationWorkerType, sw.GetID())
	eofString := sw.GetEOFString()

	for message := range consumer {
		msg := string(message.Body)
		if msg == eofString {
			log.Infof("[worker: %s][workerID: %v][status: OK] EOF received: %s", stationWorkerType, sw.GetID(), eofString)
			eofData := eof.NewEOF(sw.config.City, stationWorkerType, eofString)
			eofBytes, err := json.Marshal(eofData)
			if err != nil {
				log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error marshalling EOF message: %s", stationWorkerType, sw.GetID(), err.Error())
				return err
			}
			err = sw.rabbitMQ.PublishMessageInQueue(ctx, sw.config.EOFQueueConfig.Name, eofBytes, contentTypeJson)

			if err != nil {
				log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing EOF message: %s", stationWorkerType, sw.GetID(), err.Error())
				return err
			}
			break
		}

		log.Debugf("[worker: %s][workerID: %v][status: OK][method: ProcessInputMessages] received message %s", stationWorkerType, sw.GetID(), msg)
		err = sw.processData(ctx, msg)
		if err != nil {
			return err
		}
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] all data were processed", stationWorkerType, sw.GetID())
	return nil
}

func (sw *StationWorker) Kill() error {
	return sw.rabbitMQ.KillBadBunny()
}

// processData dataChunk is a string with the following format:
// stations,city,data1_1,data1_2,...,data1_N|stations,city,data2_1,data2_2...,data2_N|PING
// Only valid data from the received batch is sent to the next stage
func (sw *StationWorker) processData(ctx context.Context, dataChunk string) error {
	validData, err := sw.getValidDataToSend(dataChunk)
	if err != nil {
		return err
	}

	if len(validData) <= 0 {
		return nil
	}

	validDataAsBytes, err := sw.marshalDataToSend(validData)
	if err != nil {
		return err
	}

	if utils.ContainsString(sw.config.City, sw.config.IncludeCities) {
		err = sw.publishDataInExchange(ctx, validDataAsBytes, sw.config.ExchangesConfig[exchangeOutput+"city_joiner"].Name)
		if err != nil {
			log.Errorf("[worker: %s][workerID: %v][status: Error] error publishing data in Montreal Joiner", stationWorkerType, sw.GetID())
			return err
		}
	}

	validDataTuned := sw.filterDataByYears(validData) // slice with data that has a year equal to 2016 or 2017
	if len(validDataTuned) <= 0 {
		return nil
	}

	dataToSendByQuarterTuned, err := sw.marshalDataToSend(validDataTuned)
	if err != nil {
		return err
	}
	err = sw.publishDataInExchange(ctx, dataToSendByQuarterTuned, sw.config.ExchangesConfig[exchangeOutput+"year_joiner"].Name)
	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: Error] error publishing data in Year Joiner", stationWorkerType, sw.GetID())
		return err
	}

	return nil
}

func (sw *StationWorker) getStationData(data string) (*station.StationData, error) {
	dataSplit := strings.Split(data, sw.config.DataFieldDelimiter)

	stationCode, err := strconv.Atoi(dataSplit[sw.config.ValidColumnsIndexes.Code])
	if err != nil {
		log.Debugf("Invalid station code ID: %v", dataSplit[sw.config.ValidColumnsIndexes.Code])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrStationCodeType, dataErrors.ErrInvalidStationData)
	}

	latitude := dataSplit[sw.config.ValidColumnsIndexes.Latitude] // for query 2 we don't care about these values, so we cannot filter them. We add a fake value to avoid errors
	longitude := dataSplit[sw.config.ValidColumnsIndexes.Longitude]

	yearID, err := strconv.Atoi(dataSplit[sw.config.ValidColumnsIndexes.YearID])
	if err != nil {
		log.Debugf("Invalid year ID: %v", dataSplit[sw.config.ValidColumnsIndexes.YearID])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidYearIDType, dataErrors.ErrInvalidStationData)
	}

	return &station.StationData{
		Code:      stationCode,
		Name:      dataSplit[sw.config.ValidColumnsIndexes.Name],
		Latitude:  latitude,
		Longitude: longitude,
		YearID:    yearID,
	}, nil
}

// isValid returns true if the following conditions are met:
// + The station code is equal or greater than 0
// + Latitude is between -90 and 90
// + Longitude is between -180 and 180
// + Name is not the empty string
func (sw *StationWorker) isValid(stationData *station.StationData) bool {
	validData := true
	var invalidReasons []string

	if stationData.Code < 0 {
		invalidReasons = append(invalidReasons, "Station code < 0")
		validData = false
	}

	if !validData {
		log.Infof("[worker: %s][workerID: %v] Invalid data, reasons: %v", stationWorkerType, sw.GetID(), invalidReasons)
	}

	return validData
}

// getValidDataToSend returns a slice with valid data to send to the next stage.
func (sw *StationWorker) getValidDataToSend(dataChunk string) ([]*station.StationData, error) {
	dataSplit := strings.Split(dataChunk, sw.config.DataDelimiter)
	var validStationData []*station.StationData
	for _, data := range dataSplit {
		if strings.Contains(data, sw.config.EndBatchMarker) {
			log.Debug("bypassing PING")
			continue
		}

		stationData, err := sw.getStationData(data)
		if err != nil {
			if errors.Is(err, dataErrors.ErrInvalidStationData) {
				continue
			}
			return nil, err
		}

		if sw.isValid(stationData) {
			stationData.Metadata = entities.NewMetadata(sw.config.City, stationStr, stationWorkerType, "")
			validStationData = append(validStationData, stationData)
		}
	}

	return validStationData, nil
}

// marshalDataToSend returns a map with the quarters that have data to send
func (sw *StationWorker) marshalDataToSend(data []*station.StationData) ([]byte, error) {
	dataAsBytes, err := json.Marshal(data)
	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error marshaling data: %s", stationWorkerType, sw.GetID(), err.Error())
		return nil, err
	}
	return dataAsBytes, nil
}

// publishDataInExchange publish the given chunk of data in exchangeName
func (sw *StationWorker) publishDataInExchange(ctx context.Context, dataToSend []byte, exchangeName string) error {
	targetStage := utils.GetTargetStage(exchangeName)
	// OBS: it has a difference with the other workers, here we use de word 'stations' as the id, we don't have anything to get a Qid
	routingKey := fmt.Sprintf("%s.%s.%s", targetStage, sw.config.City, stationStr)
	err := sw.rabbitMQ.PublishMessageInExchange(ctx, exchangeName, routingKey, dataToSend, contentTypeJson)

	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing message in join exchange %s: %s", stationWorkerType, sw.GetID(), exchangeName, err.Error())
		return err
	}

	return nil
}

// filterDataByYears filters data that has a year that it's not part of the valid ones
func (sw *StationWorker) filterDataByYears(originalData []*station.StationData) []*station.StationData {
	var filteredData []*station.StationData
	for idx := range originalData {
		data := *originalData[idx]
		if utils.ContainsInt(data.YearID, sw.config.IncludeYears) {
			filteredData = append(filteredData, &data)
		}
	}
	return filteredData
}
