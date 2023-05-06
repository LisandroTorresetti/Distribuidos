package weather

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
	"tp1/domain/entities/weather"
	dataErrors "tp1/workers/factory/worker_type/errors"
	"tp1/workers/factory/worker_type/weather/config"
)

const (
	dateLayout        = "2006-01-02"
	weatherWorkerType = "weather-worker"
	weatherStr        = "weather"
	exchangeInput     = "exchange_input_"
	exchangeOutput    = "exchange_output_"
)

type WeatherWorker struct {
	rabbitMQ  *communication.RabbitMQ
	config    *config.WeatherConfig
	delimiter string
}

func NewWeatherWorker(weatherBrokerConfig *config.WeatherConfig, rabbitMQ *communication.RabbitMQ) *WeatherWorker {
	return &WeatherWorker{
		delimiter: ",",
		rabbitMQ:  rabbitMQ,
		config:    weatherBrokerConfig,
	}
}

// GetID returns the Weather Worker ID
func (ww *WeatherWorker) GetID() int {
	return ww.config.ID
}

// GetType returns the Weather Worker type
func (ww *WeatherWorker) GetType() string {
	return weatherWorkerType
}

// GetRoutingKeys returns the Weather Worker routing keys
func (ww *WeatherWorker) GetRoutingKeys() []string {
	return []string{
		fmt.Sprintf("%s.%s.%v", weatherStr, ww.config.City, ww.GetID()), // input routing key: weather.city.workerID
		fmt.Sprintf("%s.eof", weatherStr),                               //weather.eof
	}
}

// GetEOFString returns the Weather Worker expected EOF String
func (ww *WeatherWorker) GetEOFString() string {
	return weatherStr + "-PONG"
}

// GetEOFMessageTuned returns an EOF message with the following structure: eof.weather.city.workerID
// Possible values for city: washington, toronto, montreal
func (ww *WeatherWorker) GetEOFMessageTuned() string {
	return fmt.Sprintf("eof.%s.%s.%v", weatherStr, ww.config.City, ww.GetID())
}

// DeclareQueues declares non-anonymous queues for Weather Worker
// Queues: EOF queue
func (ww *WeatherWorker) DeclareQueues() error {
	var queues []communication.QueueDeclarationConfig
	for key, rabbitConfig := range ww.config.RabbitMQConfig[weatherStr] {
		if strings.Contains(key, "queue") {
			queues = append(queues, rabbitConfig.QueueDeclarationConfig)
		}
	}

	err := ww.rabbitMQ.DeclareNonAnonymousQueues(queues)
	if err != nil {
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] queues declared correctly!", weatherWorkerType, ww.GetID())
	return nil
}

// DeclareExchanges declares exchanges for Weather Worker
// Exchanges: weather_topic, rain_accumulator_topic
func (ww *WeatherWorker) DeclareExchanges() error {
	var exchanges []communication.ExchangeDeclarationConfig
	for key, rabbitConfig := range ww.config.RabbitMQConfig[weatherStr] {
		if strings.Contains(key, "exchange") {
			exchanges = append(exchanges, rabbitConfig.ExchangeDeclarationConfig)
		}
	}

	err := ww.rabbitMQ.DeclareExchanges(exchanges)
	if err != nil {
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] exchanges declared correctly!", weatherWorkerType, ww.GetID())
	return nil
}

// ProcessInputMessages process all messages that Weather Worker receives
func (ww *WeatherWorker) ProcessInputMessages() error {
	exchangeName := exchangeInput + weatherStr
	routingKeys := ww.GetRoutingKeys()

	consumer, err := ww.rabbitMQ.GetExchangeConsumer(exchangeName, routingKeys)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Debugf("[worker: %s][workerID: %v][status: OK]start consuming messages", weatherWorkerType, ww.GetID())
	eofString := ww.GetEOFString()

	for message := range consumer {
		msg := string(message.Body)
		if msg == eofString {
			log.Infof("[worker: %s][workerID: %v][status: OK] EOF received", weatherWorkerType, ww.GetID())
			targetQueue := fmt.Sprintf("%s.eof.manager", weatherStr) // ToDo: create a better string if its necessary
			eofMessage := []byte(ww.GetEOFMessageTuned())
			err = ww.rabbitMQ.PublishMessageInQueue(ctx, targetQueue, eofMessage, "text/plain")

			if err != nil {
				log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing EOF message: %s", weatherWorkerType, ww.GetID(), err.Error())
				return err
			}
			break
		}

		log.Debugf("[worker: %s][workerID: %v][status: OK][method: ProcessInputMessages] received message %s", weatherWorkerType, ww.GetID(), msg)
		err = ww.processData(ctx, msg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ww *WeatherWorker) Kill() error {
	return ww.rabbitMQ.KillBadBunny()
}

// processData data is a string with the following format:
// weather,city,data1_1,data1_2,...,data1_N|weather,city,data2_1,data2_2...,data2_N|PING
// Only valid data from the received batch is sent to the next stage
func (ww *WeatherWorker) processData(ctx context.Context, dataChunk string) error {
	dataSplit := strings.Split(dataChunk, "|")
	var dataToSend []*weather.WeatherData
	for _, data := range dataSplit {
		if strings.Contains(data, "PING") {
			log.Debug("bypassing PING")
			continue
		}

		weatherData, err := ww.getWeatherData(data)
		if err != nil {
			if errors.Is(err, dataErrors.ErrInvalidWeatherData) {
				continue
			}
			return err
		}

		if ww.isValid(weatherData) {
			weatherData.City = ww.config.City
			weatherData.Type = weatherStr
			dataToSend = append(dataToSend, weatherData)
		}
	}

	if len(dataToSend) <= 0 {
		return nil
	}

	dataAsBytes, err := json.Marshal(dataToSend)
	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error][method: publishMessage] error marshaling data: %s", weatherWorkerType, ww.GetID(), err.Error())
		return err
	}

	targetQueue := fmt.Sprintf("%s.%s.join", weatherStr, ww.config.City) // ToDo: we need something when we have to publish in multiple queues, maybe an array of queue names
	err = ww.rabbitMQ.PublishMessageInQueue(ctx, targetQueue, dataAsBytes, "application/json")

	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing message in join queue: %s", weatherWorkerType, ww.GetID(), err.Error())
		return err
	}
	return nil
}

func (ww *WeatherWorker) getWeatherData(data string) (*weather.WeatherData, error) {
	dataSplit := strings.Split(data, ww.delimiter)
	date, err := time.Parse(dateLayout, dataSplit[ww.config.ValidColumnsIndexes.Date])
	if err != nil {
		log.Debugf("Invalid date %s", dataSplit[ww.config.ValidColumnsIndexes.Date])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidDate, dataErrors.ErrInvalidWeatherData)
	}

	date = date.AddDate(0, 0, -1)

	rainfall, err := strconv.ParseFloat(dataSplit[ww.config.ValidColumnsIndexes.Rainfall], 64)
	if err != nil {
		log.Debugf("Invalid rainfall type")
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidRainfallType, dataErrors.ErrInvalidWeatherData)
	}

	return &weather.WeatherData{
		Date:     date,
		Rainfall: rainfall,
	}, nil
}

// isValid returns true if the following conditions are met:
// + Rainfall is greater than 30mm
func (ww *WeatherWorker) isValid(weatherData *weather.WeatherData) bool {
	return weatherData.Rainfall > ww.config.RainfallThreshold
}
