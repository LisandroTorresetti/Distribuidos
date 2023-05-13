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
	"tp1/utils"
	dataErrors "tp1/workers/factory/worker_type/errors"
	"tp1/workers/factory/worker_type/weather/config"
)

const (
	dateLayout           = "2006-01-02"
	weatherWorkerType    = "weather-worker"
	weatherStr           = "weather"
	exchangePostfix      = "-topic"
	outputTarget         = "output"
	contentTypeJson      = "application/json"
	contentTypePlainText = "text/plain"
)

type WeatherWorker struct {
	rabbitMQ  *communication.RabbitMQ
	config    *config.WeatherWorkerConfig
	delimiter string
}

func NewWeatherWorker(weatherWorkerConfig *config.WeatherWorkerConfig, rabbitMQ *communication.RabbitMQ) *WeatherWorker {
	return &WeatherWorker{
		delimiter: ",",
		rabbitMQ:  rabbitMQ,
		config:    weatherWorkerConfig,
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
		fmt.Sprintf("eof.%s.%s", weatherStr, ww.config.City),            //eof.dataType.city
	}
}

// GetEOFString returns the Weather Worker expected EOF String
func (ww *WeatherWorker) GetEOFString() string {
	return fmt.Sprintf("eof.%s.%s", weatherStr, ww.config.City)
}

// DeclareQueues declares non-anonymous queues for Weather Worker
// Queues: EOF queue
func (ww *WeatherWorker) DeclareQueues() error {
	err := ww.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{ww.config.EOFQueueConfig})
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
	for _, exchangeConfig := range ww.config.ExchangesConfig {
		exchanges = append(exchanges, exchangeConfig)
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
	exchangeName := weatherStr + exchangePostfix
	routingKeys := ww.GetRoutingKeys()

	consumer, err := ww.rabbitMQ.GetExchangeConsumer(exchangeName, routingKeys)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Infof("[worker: %s][workerID: %v][status: OK]start consuming messages", weatherWorkerType, ww.GetID())
	eofString := ww.GetEOFString()

	for message := range consumer {
		msg := string(message.Body)
		if msg == eofString {
			log.Infof("[worker: %s][workerID: %v][status: OK] EOF received: %s", weatherWorkerType, ww.GetID(), eofString)
			eofMessage := []byte(eofString)
			err = ww.rabbitMQ.PublishMessageInQueue(ctx, ww.config.EOFQueueConfig.Name, eofMessage, contentTypePlainText)

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

	log.Infof("[worker: %s][workerID: %v][status: OK] all data were processed", weatherWorkerType, ww.GetID())
	return nil
}

func (ww *WeatherWorker) Kill() error {
	return ww.rabbitMQ.KillBadBunny()
}

// processData data is a string with the following format:
// weather,city,data1_1,data1_2,...,data1_N|weather,city,data2_1,data2_2...,data2_N|PING
// Only valid data from the received batch is sent to the next stage
func (ww *WeatherWorker) processData(ctx context.Context, dataChunk string) error {
	quartersMap, err := ww.getValidDataToSend(dataChunk)
	if err != nil {
		return err
	}

	if !hasDataToSend(quartersMap) {
		return nil
	}

	dataToSendMap, err := ww.marshalDataToSend(quartersMap)
	if err != nil {
		return err
	}

	err = ww.publishData(ctx, dataToSendMap)
	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: Error] error publishing data in output exchanges", weatherWorkerType, ww.GetID())
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

// getValidDataToSend returns a map organized by quarters (Q1, Q2, Q3, Q4) with valid data to send to the next stage.
func (ww *WeatherWorker) getValidDataToSend(dataChunk string) (map[string][]*weather.WeatherData, error) {
	dataSplit := strings.Split(dataChunk, "|")
	quartersMap := getQuartersMap()
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
			return nil, err
		}

		if ww.isValid(weatherData) {
			weatherData.City = ww.config.City
			weatherData.Type = weatherStr
			quarterID := utils.GetQuarter(int(weatherData.Date.Month()))
			quartersMap[quarterID] = append(quartersMap[quarterID], weatherData)
		}
	}
	return quartersMap, nil
}

// marshalDataToSend returns a map with the quarters that have data to send
func (ww *WeatherWorker) marshalDataToSend(data map[string][]*weather.WeatherData) (map[string][]byte, error) {
	dataToSendMap := make(map[string][]byte)
	for key, value := range data {
		if len(value) > 0 {
			dataAsBytes, err := json.Marshal(value)
			if err != nil {
				log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error marshaling data: %s", weatherWorkerType, ww.GetID(), err.Error())
				return nil, err
			}
			dataToSendMap[key] = dataAsBytes
		}
	}

	return dataToSendMap, nil
}

// publishData publishes data in all output exchanges related with the Weather Worker
func (ww *WeatherWorker) publishData(ctx context.Context, dataToSendMap map[string][]byte) error {
	for exchangeName, _ := range ww.config.ExchangesConfig {
		if strings.Contains(exchangeName, outputTarget) {
			targetStage := utils.GetTargetStage(exchangeName)
			for quarterID, dataToSend := range dataToSendMap {
				routingKey := fmt.Sprintf("%s.%s.%s", targetStage, ww.config.City, quarterID)
				err := ww.rabbitMQ.PublishMessageInExchange(ctx, exchangeName, routingKey, dataToSend, contentTypeJson)

				if err != nil {
					log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing message in join exchange: %s", weatherWorkerType, ww.GetID(), err.Error())
					return err
				}
			}
		}
	}

	return nil
}

func getQuartersMap() map[string][]*weather.WeatherData {
	quartersMap := make(map[string][]*weather.WeatherData)
	quartersMap["Q1"] = []*weather.WeatherData{}
	quartersMap["Q2"] = []*weather.WeatherData{}
	quartersMap["Q3"] = []*weather.WeatherData{}
	quartersMap["Q4"] = []*weather.WeatherData{}

	return quartersMap
}

func hasDataToSend(data map[string][]*weather.WeatherData) bool {
	for _, value := range data {
		if len(value) > 0 {
			return true
		}
	}
	return false
}

