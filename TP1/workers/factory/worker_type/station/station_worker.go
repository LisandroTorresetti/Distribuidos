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
	"tp1/domain/entities/station"
	dataErrors "tp1/workers/factory/worker_type/errors"
	"tp1/workers/factory/worker_type/station/config"
)

const (
	dateLayout        = "2006-01-02"
	latitudeBound     = 90
	longitudeBound    = 180
	stationWorkerType = "station-worker"
	stationStr        = "stations"
	exchangeInput     = "exchange_input_"
)

type StationWorker struct {
	rabbitMQ  *communication.RabbitMQ
	config    *config.StationConfig
	delimiter string
}

func NewStationWorker(stationWorkerConfig *config.StationConfig, rabbitMQ *communication.RabbitMQ) *StationWorker {
	return &StationWorker{
		delimiter: ",",
		rabbitMQ:  rabbitMQ,
		config:    stationWorkerConfig,
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
		fmt.Sprintf("%s.eof", stationStr),                               //weather.eof
	}
}

// GetEOFString returns the Station Worker expected EOF String
func (sw *StationWorker) GetEOFString() string {
	return stationStr + "-PONG"
}

// GetEOFMessageTuned returns an EOF message with the following structure: eof.stations.city.workerID
// Possible values for city: washington, toronto, montreal
func (sw *StationWorker) GetEOFMessageTuned() string {
	return fmt.Sprintf("eof.%s.%s.%v", stationStr, sw.config.City, sw.GetID())
}

// DeclareQueues declares non-anonymous queues for Station Worker
// Queues: EOF queue
func (sw *StationWorker) DeclareQueues() error {
	var queues []communication.QueueDeclarationConfig
	for key, rabbitConfig := range sw.config.RabbitMQConfig[stationStr] {
		if strings.Contains(key, "queue") {
			queues = append(queues, rabbitConfig.QueueDeclarationConfig)
		}
	}

	err := sw.rabbitMQ.DeclareNonAnonymousQueues(queues)
	if err != nil {
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] queues declared correctly!", stationWorkerType, sw.GetID())
	return nil
}

// DeclareExchanges declares exchanges for Station Worker
// Exchanges: weather_topic, rain_accumulator_topic
func (sw *StationWorker) DeclareExchanges() error {
	var exchanges []communication.ExchangeDeclarationConfig
	for key, rabbitConfig := range sw.config.RabbitMQConfig[stationStr] {
		if strings.Contains(key, "exchange") {
			exchanges = append(exchanges, rabbitConfig.ExchangeDeclarationConfig)
		}
	}

	err := sw.rabbitMQ.DeclareExchanges(exchanges)
	if err != nil {
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] exchanges declared correctly!", stationWorkerType, sw.GetID())
	return nil
}

// ProcessInputMessages process all messages that Station Worker receives
func (sw *StationWorker) ProcessInputMessages() error {
	exchangeName := exchangeInput + stationStr
	routingKeys := sw.GetRoutingKeys()

	consumer, err := sw.rabbitMQ.GetExchangeConsumer(exchangeName, routingKeys)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Debugf("[worker: %s][workerID: %v][status: OK]start consuming messages", stationWorkerType, sw.GetID())
	eofString := sw.GetEOFString()

	for message := range consumer {
		msg := string(message.Body)
		if msg == eofString {
			log.Infof("[worker: %s][workerID: %v][status: OK] EOF received", stationWorkerType, sw.GetID())
			targetQueue := fmt.Sprintf("%s.eof.manager", stationStr) // ToDo: create a better string if its necessary
			eofMessage := []byte(sw.GetEOFMessageTuned())
			err = sw.rabbitMQ.PublishMessageInQueue(ctx, targetQueue, eofMessage, "text/plain")

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

	return nil
}

func (sw *StationWorker) Kill() error {
	return sw.rabbitMQ.KillBadBunny()
}

// processData data is a string with the following format:
// weather,city,data1_1,data1_2,...,data1_N|weather,city,data2_1,data2_2...,data2_N|PING
// Only valid data from the received batch is sent to the next stage
func (sw *StationWorker) processData(ctx context.Context, dataChunk string) error {
	dataSplit := strings.Split(dataChunk, "|")
	var dataToSend []*station.StationData
	for _, data := range dataSplit {
		if strings.Contains(data, "PING") {
			log.Debug("bypassing PING")
			continue
		}

		weatherData, err := sw.getStationData(data)
		if err != nil {
			if errors.Is(err, dataErrors.ErrInvalidWeatherData) {
				continue
			}
			return err
		}

		if sw.isValid(weatherData) {
			weatherData.City = sw.config.City
			weatherData.Type = stationStr
			dataToSend = append(dataToSend, weatherData)
		}
	}

	if len(dataToSend) <= 0 {
		return nil
	}

	dataAsBytes, err := json.Marshal(dataToSend)
	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error marshaling data: %s", stationWorkerType, sw.GetID(), err.Error())
		return err
	}

	targetQueue := fmt.Sprintf("%s.%s.join", stationStr, sw.config.City) // ToDo: we need something when we have to publish in multiple queues, maybe an array of queue names
	err = sw.rabbitMQ.PublishMessageInQueue(ctx, targetQueue, dataAsBytes, "application/json")

	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing message in join queue: %s", stationWorkerType, sw.GetID(), err.Error())
		return err
	}
	return nil
}

func (sw *StationWorker) getStationData(data string) (*station.StationData, error) {
	dataSplit := strings.Split(data, sw.delimiter)

	stationCode, err := strconv.Atoi(dataSplit[sw.config.ValidColumnsIndexes.Code])
	if err != nil {
		log.Debugf("Invalid station code ID: %v", dataSplit[sw.config.ValidColumnsIndexes.Code])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrStationCodeType, dataErrors.ErrInvalidStationData)
	}

	latitude, err := strconv.ParseFloat(dataSplit[sw.config.ValidColumnsIndexes.Latitude], 64)
	if err != nil {
		log.Debugf("Invalid latitude: %v", dataSplit[sw.config.ValidColumnsIndexes.Latitude])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidLatitude, dataErrors.ErrInvalidStationData)
	}

	longitude, err := strconv.ParseFloat(dataSplit[sw.config.ValidColumnsIndexes.Longitude], 64)
	if err != nil {
		log.Debugf("Invalid longitude: %v", dataSplit[sw.config.ValidColumnsIndexes.Longitude])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidLongitude, dataErrors.ErrInvalidStationData)
	}

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
// + The station code is greater than 0
// + Latitude is between -90 and 90
// + Longitude is between -180 and 180
// + Name is not the empty string
func (sw *StationWorker) isValid(stationData *station.StationData) bool {
	validData := true
	var invalidReasons []string

	latitude := stationData.Latitude
	longitude := stationData.Longitude

	if !(-latitudeBound <= latitude && latitude <= latitudeBound) {
		validData = false
		invalidReasons = append(invalidReasons, "latitude out of bound")
	}

	if !(longitudeBound <= longitude && longitude <= longitudeBound) {
		validData = false
		invalidReasons = append(invalidReasons, "longitude out of bound")
	}
	if stationData.Code <= 0 {
		invalidReasons = append(invalidReasons, "Station code < 0")
		validData = false
	}

	if !validData {
		log.Infof("[worker: %s][workerID: %v] Invalid data, reasons: %v", stationWorkerType, sw.GetID(), invalidReasons)
	}

	return validData
}
