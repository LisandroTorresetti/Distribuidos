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
	"tp1/domain/entities/trip"
	dataErrors "tp1/workers/factory/worker_type/errors"
	"tp1/workers/factory/worker_type/trip/config"
)

const (
	dateLayout      = "2006-01-02"
	tripWorkerType  = "trips-worker"
	tripStr         = "trips"
	exchangePostfix = "-topic"
)

type TripWorker struct {
	rabbitMQ  *communication.RabbitMQ
	config    *config.TripWorkerConfig
	delimiter string
}

func NewTripWorker(tripWorkerConfig *config.TripWorkerConfig, rabbitMQ *communication.RabbitMQ) *TripWorker {
	return &TripWorker{
		rabbitMQ:  rabbitMQ,
		delimiter: ",",
		config:    tripWorkerConfig,
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

// GetRoutingKeys returns the Trip Worker routing keys
func (tw *TripWorker) GetRoutingKeys() []string {
	return []string{
		fmt.Sprintf("%s.%s.%v", tripStr, tw.config.City, tw.GetID()), // input routing key: trips.city.workerID
		fmt.Sprintf("eof.%s.%s", tripStr, tw.config.City),            //eof.dataType.city
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

func (tw *TripWorker) DeclareExchanges() error {
	var exchanges []communication.ExchangeDeclarationConfig
	for _, exchange := range tw.config.ExchangesConfig {
		exchanges = append(exchanges, exchange)
	}

	err := tw.rabbitMQ.DeclareExchanges(exchanges)
	if err != nil {
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] exchanges declared correctly!", tripStr, tw.GetID())
	return nil
}

// ProcessInputMessages process all messages that Trip Worker receives
func (tw *TripWorker) ProcessInputMessages() error {
	exchangeName := tripStr + exchangePostfix
	routingKeys := tw.GetRoutingKeys()

	consumer, err := tw.rabbitMQ.GetExchangeConsumer(exchangeName, routingKeys)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Debugf("[worker: %s][workerID: %v][status: OK]start consuming messages", tripWorkerType, tw.GetID())
	eofString := tw.GetEOFString()

	for message := range consumer {
		msg := string(message.Body)
		if msg == eofString {
			log.Infof("[worker: %s][workerID: %v][status: OK] EOF received: %s", tripWorkerType, tw.GetID(), eofString)
			eofMessage := []byte(eofString)
			err = tw.rabbitMQ.PublishMessageInQueue(ctx, tw.config.EOFQueueConfig.Name, eofMessage, "text/plain")

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

	return nil
}

func (tw *TripWorker) Kill() error {
	return tw.rabbitMQ.KillBadBunny()
}

// processData data is a string with the following format:
// trips,city,data1_1,data1_2,...,data1_N|trips,city,data2_1,data2_2...,data2_N|PING
// Only valid data from the received batch is sent to the next stage
func (tw *TripWorker) processData(ctx context.Context, dataChunk string) error {
	dataSplit := strings.Split(dataChunk, "|")
	var dataToSend []*trip.TripData
	for _, data := range dataSplit {
		if strings.Contains(data, "PING") {
			log.Debug("bypassing PING")
			continue
		}

		tripData, err := tw.getTripData(data)
		if err != nil {
			if errors.Is(err, dataErrors.ErrInvalidTripData) {
				continue
			}
			return err
		}

		if tw.isValid(tripData) {
			tripData.City = tw.config.City
			tripData.Type = tripStr
			dataToSend = append(dataToSend, tripData)
		}
	}

	if len(dataToSend) <= 0 {
		return nil
	}

	_, err := json.Marshal(dataToSend)
	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error marshaling data: %s", tripWorkerType, tw.GetID(), err.Error())
		return err
	}

	log.Debugf("[worker: %s][workerID: %v] RECIBI DATA LICHITA: %s", tripWorkerType, tw.GetID(), dataChunk)

	//targetQueues := fmt.Sprintf("%s.%s.join", tripStr, tw.config.City) // ToDo: we need something when we have to publish in multiple queues, maybe an array of queue names
	/*var targetQueues []string // Fixme: add values to this slice
	for _, targetQueue := range targetQueues {
		err = tw.rabbitMQ.PublishMessageInQueue(ctx, targetQueue, dataAsBytes, "application/json")

		// Fixme: we have to send this message to:
		// Rain Joiner, Year Filter, DuplicateJoiner

		if err != nil {
			log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing message in join queue: %s", tripWorkerType, tw.GetID(), err.Error())
			return err
		}
	}*/

	return nil
}

func (tw *TripWorker) getTripData(data string) (*trip.TripData, error) {
	dataSplit := strings.Split(data, tw.delimiter)
	startDateStr := dataSplit[tw.config.ValidColumnsIndexes.StartDate] // To avoid hours:minutes:seconds
	startDateStr = strings.Split(startDateStr, " ")[0]
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

func getRandomID() int {
	/*// initialize the random number generator
	rand.Seed(time.Now().UnixNano())

	// generate a random number between 1 and 3
	return rand.Intn(3) + 1*/
	return 1
}
