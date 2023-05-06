package trip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
	"tp1/domain/communication"
	"tp1/domain/entities/trip"
	dataErrors "tp1/workers/factory/worker_type/errors"
	"tp1/workers/factory/worker_type/trip/config"
)

const (
	dateLayout     = "2006-01-02"
	tripWorkerType = "trips-worker"
	tripStr        = "trips"
	exchangeInput  = "exchange_input_"
	exchangeOutput = "exchange_output_"
)

type TripWorker struct {
	config    *config.TripConfig
	delimiter string
}

func NewTripWorker(tripWorkerConfig *config.TripConfig) *TripWorker {
	return &TripWorker{
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
		fmt.Sprintf("%s.eof", tripStr),
	}
}

// GetEOFString returns the Trip Worker expected EOF String
func (tw *TripWorker) GetEOFString() string {
	return tripStr + "-PONG"
}

// DeclareQueues declares non-anonymous queues for Trip Worker
func (tw *TripWorker) DeclareQueues(channel *amqp.Channel) error {
	rabbitQueueConfig, ok := tw.config.RabbitMQConfig[tripStr][exchangeInput+tripStr]
	if !ok {
		return fmt.Errorf("[worker: %s][status: error] config with key %s does not exists", tripWorkerType, exchangeInput+tripStr)
	}

	queueName := rabbitQueueConfig.QueueDeclarationConfig.Name
	_, err := channel.QueueDeclare(
		queueName,
		rabbitQueueConfig.QueueDeclarationConfig.Durable,
		rabbitQueueConfig.QueueDeclarationConfig.DeleteWhenUnused,
		rabbitQueueConfig.QueueDeclarationConfig.Exclusive,
		rabbitQueueConfig.QueueDeclarationConfig.NoWait,
		nil,
	)

	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error] error declaring queue %s: %s", tripWorkerType, tw.GetID(), queueName, err.Error())
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] queue %s declared correctly!", tripWorkerType, tw.GetID(), queueName)

	return nil
}

func (tw *TripWorker) DeclareExchanges(channel *amqp.Channel) error {
	for key, rabbitConfig := range tw.config.RabbitMQConfig[tripStr] {
		if strings.Contains(key, "exchange") {
			exchangeName := rabbitConfig.ExchangeDeclarationConfig.Name
			err := channel.ExchangeDeclare(
				exchangeName,
				rabbitConfig.ExchangeDeclarationConfig.Type,
				rabbitConfig.ExchangeDeclarationConfig.Durable,
				rabbitConfig.ExchangeDeclarationConfig.AutoDeleted,
				rabbitConfig.ExchangeDeclarationConfig.Internal,
				rabbitConfig.ExchangeDeclarationConfig.NoWait,
				nil,
			)
			if err != nil {
				log.Errorf("[worker: %s][workerID: %v][status: error] error declaring exchange %s: %s", tripWorkerType, tw.GetID(), exchangeName, err.Error())
				return err
			}
		}
	}
	log.Infof("[worker: %s][workerID: %v][status: OK] exchanges declared correctly!", tripWorkerType, tw.GetID())
	return nil
}

// ProcessInputMessages process all messages that Trip Worker receives
func (tw *TripWorker) ProcessInputMessages(channel *amqp.Channel) error {
	configKey := exchangeInput + tripStr
	rabbitMQ, ok := tw.config.RabbitMQConfig[tripStr][configKey]
	if !ok {
		return fmt.Errorf("[worker: %s][]workerID: %v[status: error] cannot find RabbitMQ config for key %s", tripWorkerType, tw.GetID(), configKey)
	}

	anonymousQueue, err := channel.QueueDeclare(
		"",
		rabbitMQ.QueueDeclarationConfig.Durable,
		rabbitMQ.QueueDeclarationConfig.DeleteWhenUnused,
		rabbitMQ.QueueDeclarationConfig.Exclusive,
		rabbitMQ.QueueDeclarationConfig.NoWait,
		nil,
	)

	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error] error declaring anonymus queue: %s", tripWorkerType, tw.GetID(), err.Error())
		return err
	}

	// Binding
	routingKeys := tw.GetRoutingKeys()

	for _, routingKey := range routingKeys {
		log.Debugf("LICHA: routing key %s", routingKey)
		err = channel.QueueBind(
			anonymousQueue.Name,
			routingKey,
			rabbitMQ.ExchangeDeclarationConfig.Name,
			rabbitMQ.ExchangeDeclarationConfig.NoWait,
			nil,
		)
		if err != nil {
			log.Errorf("[worker: %s][workerID: %v][status: error] error binding routing key %s: %s", tripWorkerType, tw.GetID(), routingKey, err.Error())
			return err
		}
	}

	messages, err := channel.Consume(
		anonymousQueue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error] error getting consumer: %s", tripWorkerType, tw.GetID(), err.Error())
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Debugf("[worker: %s][workerID: %v][status: OK]start consuming messages", tripWorkerType, tw.GetID())
	eofString := tw.GetEOFString()

	for d := range messages {
		sms := string(d.Body)
		if sms == eofString {
			log.Infof("[worker: %s][workerID: %v][status: OK] EOF received", tripWorkerType, tw.GetID())
			// WARNING: from now, the EOF message will have the following structure eof.trips.city.id. And will be used as the routing key too
			newEOFMessage := tw.getEOFMessageTuned()
			err = tw.publishInAllExchanges(ctx, channel, []byte(newEOFMessage), newEOFMessage)
			if err != nil {
				log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing message: %s", tripWorkerType, tw.GetID(), err.Error())
				return err
			}

			break
		}

		log.Debugf("[worker: %s][workerID: %v][status: OK][method: ProcessInputMessages] received message %s", tripWorkerType, tw.GetID(), sms)
		smsSplit := strings.Split(sms, "|") // We received a batch of data here
		for _, smsData := range smsSplit {
			if strings.Contains(smsData, "PING") {
				log.Debug("bypassing PING")
				continue
			}

			err = tw.processData(ctx, channel, smsData)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tw *TripWorker) GetRabbitConfig(workerType string, configKey string) (communication.RabbitMQ, bool) {
	//TODO implement me
	panic("implement me")
}

func (tw *TripWorker) ProcessData(ctx context.Context, channel *amqp.Channel, data string) error {
	//TODO implement me
	panic("implement me")
}

func (tw *TripWorker) processData(ctx context.Context, channel *amqp.Channel, data string) error {
	log.Infof("[worker: %s][workerID: %v][status: OK] Processing WEATHER data: %s", tripWorkerType, tw.GetID(), data)
	tripData, err := tw.getTripData(data)
	if err != nil {
		if errors.Is(err, dataErrors.ErrInvalidWeatherData) {
			return nil
		}
		return err
	}

	if tw.isValid(tripData) {
		log.Infof("[worker: %s][workerID: %v][status: OK][method: processData]: valid weather data %+v", tripWorkerType, tw.GetID(), tripData)
		tripData.City = tw.config.City
		tripData.Type = tripStr
		dataAsBytes, err := json.Marshal(tripData)
		if err != nil {
			log.Errorf("[worker: %s][workerID: %v][status: error][method: publishMessage] error marshaling data: %s", tripWorkerType, tw.GetID(), err.Error())
			return err
		}

		err = tw.publishInAllExchanges(ctx, channel, dataAsBytes, "")
		if err != nil {
			log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing message: %s", tripWorkerType, tw.GetID(), err.Error())
			return err
		}
		return nil
	}

	return nil
}

func (tw *TripWorker) getTripData(data string) (*trip.TripData, error) {
	dataSplit := strings.Split(data, tw.delimiter)
	startDate, err := time.Parse(dateLayout, dataSplit[tw.config.ValidColumnsIndexes.StartDate])
	if err != nil {
		log.Debugf("Invalid start date: %v", dataSplit[tw.config.ValidColumnsIndexes.StartDate])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidDate, dataErrors.ErrInvalidTripData)
	}

	endDate, err := time.Parse(dateLayout, dataSplit[tw.config.ValidColumnsIndexes.EndDate])
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

// publishInAllExchanges has to publish in "rain_accumulator_topic", "year_filter_topic" and "montreal_filter_topic"
// ToDo: maybe we can add more logic here, eg: before publishing to year_filter_topic we can check if the year is between 2016 and 2017
func (tw *TripWorker) publishInAllExchanges(ctx context.Context, channel *amqp.Channel, data []byte, routingKey string) error {
	for exchangeName, rabbitConfig := range tw.config.RabbitMQConfig[tripStr] {
		auxRoutingKey := routingKey
		if strings.Contains(exchangeName, exchangeOutput) {
			if auxRoutingKey == "" {
				auxRoutingKey = tw.getRoutingKeyForExchange(exchangeName)
			}
			err := tw.publishMessage(ctx, channel, data, routingKey, rabbitConfig)
			if err != nil {
				log.Errorf("[worker: %s][workerID: %v][status: error][method: publishInAllExchanges] error publishing message in exchange %s: %s", tripWorkerType, tw.GetID(), exchangeName, err.Error())
				return err
			}
			log.Infof("[worker: %s][workerID: %v][status: OK][method: publishInAllExchanges] publish message correctly in exchange %s", tripWorkerType, tw.GetID(), exchangeName)
		}
	}

	log.Infof("[worker: %s][workerID: %v][status: OK][method: publishInAllExchanges] publish message correctly in all exchanges!", tripWorkerType, tw.GetID())
	return nil
}

// ToDo: maybe we can add more logic here, eg: before publishing to year_filter_topic we can check if the year is between 2016 and 2017
func (tw *TripWorker) publishMessage(ctx context.Context, channel *amqp.Channel, data []byte, routingKey string, rabbitMQConfig communication.RabbitMQ) error {
	publishConfig := rabbitMQConfig.PublishingConfig
	log.Infof("[worker: %s][workerID: %v][status: OK][method: publishMessage]Publishing message in exchange %s", tripWorkerType, tw.GetID(), publishConfig.Exchange)

	return channel.PublishWithContext(ctx,
		publishConfig.Exchange,
		routingKey,
		publishConfig.Mandatory,
		publishConfig.Immediate,
		amqp.Publishing{
			ContentType: publishConfig.ContentType,
			Body:        data,
		},
	)
}

// getEOFMessageTuned returns an EOF message with the following structure: eof.trips.city.workerID
// Possible values for city: washington, toronto, montreal
func (tw *TripWorker) getEOFMessageTuned() string {
	return fmt.Sprintf("eof.%s.%s.%v", tripStr, tw.config.City, tw.GetID())
}

// getRoutingKeyForExchange returns the routing key for a given exchange.
// Outputs:
func (tw *TripWorker) getRoutingKeyForExchange(exchangeName string) string {
	randomID := getRandomID()
	if exchangeName == "exchange_output_trips_year_filter" {
		return fmt.Sprintf("yearfilter.%s.%v", tw.config.City, randomID) // yearfilter.city.id
	}

	if exchangeName == "exchange_output_trips_rain" {
		return fmt.Sprintf("rain.%s.%v", tw.config.City, randomID) // rain.city.id
	}

	if exchangeName == "exchange_output_trips_montreal_filter" {
		return fmt.Sprintf("montrealfilter.%s.%v", tw.config.City, randomID) // montrealfilter.id
	}

	panic(fmt.Sprintf("[worker: %s][workerID: %v] invalid exchange name %s", tripWorkerType, tw.GetID(), exchangeName))
}

func getRandomID() int {
	/*// initialize the random number generator
	rand.Seed(time.Now().UnixNano())

	// generate a random number between 1 and 3
	return rand.Intn(3) + 1*/
	return 1
}
