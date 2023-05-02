package weather

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
	config    *config.WeatherConfig
	delimiter string
}

func NewWeatherWorker(weatherBrokerConfig *config.WeatherConfig) *WeatherWorker {
	return &WeatherWorker{
		delimiter: ",",
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
		fmt.Sprintf("%s.eof", weatherStr),
	}
}

// GetEOFString returns the Weather Worker expected EOF String
func (ww *WeatherWorker) GetEOFString() string {
	return weatherStr + "-PONG"
}

// DeclareQueues declares non-anonymous queues for Weather Worker
func (ww *WeatherWorker) DeclareQueues(channel *amqp.Channel) error {
	rabbitQueueConfig, ok := ww.config.RabbitMQConfig[weatherStr][exchangeInput+weatherStr]
	if !ok {
		return fmt.Errorf("[worker: %s][status: error] config with key %s does not exists", weatherWorkerType, exchangeInput+weatherStr)
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
		log.Errorf("[worker: %s][workerID: %v][status: error] error declaring queue %s: %s", weatherWorkerType, ww.GetID(), queueName, err.Error())
		return err
	}

	log.Infof("[worker: %s][workerID: %v][status: OK] queue %s declared correctly!", weatherWorkerType, ww.GetID(), queueName)

	return nil
}

func (ww *WeatherWorker) DeclareExchanges(channel *amqp.Channel) error {
	for key, rabbitConfig := range ww.config.RabbitMQConfig[weatherStr] {
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
				log.Errorf("[worker: %s][workerID: %v][status: error] error declaring exchange %s: %s", weatherWorkerType, ww.GetID(), exchangeName, err.Error())
				return err
			}
		}
	}
	log.Errorf("[worker: %s][workerID: %v][status: error] exchanges declared correctly!", weatherWorkerType, ww.GetID())
	return nil
}

// ProcessInputMessages process all messages that Weather Worker receives
func (ww *WeatherWorker) ProcessInputMessages(channel *amqp.Channel) error {
	configKey := exchangeInput + weatherStr
	rabbitMQ, ok := ww.config.RabbitMQConfig[weatherStr][configKey]
	if !ok {
		return fmt.Errorf("[worker: %s][]workerID: %v[status: error] cannot find RabbitMQ config for key %s", weatherWorkerType, ww.GetID(), configKey)
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
		log.Errorf("[worker: %s][workerID: %v][status: error] error declaring anonymus queue: %s", weatherWorkerType, ww.GetID(), err.Error())
		return err
	}

	// Binding
	routingKeys := ww.GetRoutingKeys()

	for _, routingKey := range routingKeys {
		err = channel.QueueBind(
			anonymousQueue.Name,
			routingKey,
			rabbitMQ.ExchangeDeclarationConfig.Name,
			rabbitMQ.ExchangeDeclarationConfig.NoWait,
			nil,
		)
		if err != nil {
			log.Errorf("[worker: %s][workerID: %v][status: error] error binding routing key %s: %s", weatherWorkerType, ww.GetID(), routingKey, err.Error())
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
		log.Errorf("[worker: %s][workerID: %v][status: error] error getting consumer: %s", weatherWorkerType, ww.GetID(), err.Error())
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Debugf("[worker: %s][workerID: %v][status: OK]start consuming messages", weatherWorkerType, ww.GetID())
	eofString := ww.GetEOFString()

	for d := range messages {
		sms := string(d.Body)
		if sms == eofString {
			log.Infof("[worker: %s][workerID: %v][status: OK] EOF received", weatherWorkerType, ww.GetID())
			break
		}

		log.Debugf("[worker: %s][workerID: %v][status: OK][method: ProcessInputMessages] received message %s", weatherWorkerType, ww.GetID(), sms)
		smsSplit := strings.Split(sms, "|") // We received a batch of data here
		for _, smsData := range smsSplit {
			if strings.Contains(smsData, "PING") {
				log.Debug("bypassing PING")
				continue
			}

			err = ww.processData(ctx, channel, smsData)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (ww *WeatherWorker) GetRabbitConfig(workerType string, configKey string) (communication.RabbitMQ, bool) {
	//TODO implement me
	panic("implement me")
}

func (ww *WeatherWorker) ProcessData(ctx context.Context, channel *amqp.Channel, data string) error {
	//TODO implement me
	panic("implement me")
}

func (ww *WeatherWorker) processData(ctx context.Context, channel *amqp.Channel, data string) error {
	log.Infof("[worker: %s][workerID: %v][status: OK] Processing WEATHER data: %s", weatherWorkerType, ww.GetID(), data)
	weatherData, err := ww.getWeatherData(data)
	if err != nil {
		if errors.Is(err, dataErrors.ErrInvalidWeatherData) {
			return nil
		}
		return err
	}

	if ww.isValid(weatherData) {
		log.Infof("[worker: %s][workerID: %v][status: OK][method: processData]: valid weather data %+v", weatherWorkerType, ww.GetID(), weatherData)
		weatherData.City = ww.config.City
		weatherData.Type = weatherStr
		dataAsBytes, err := json.Marshal(weatherData)
		if err != nil {
			log.Errorf("[worker: %s][workerID: %v][status: error][method: publishMessage] error marshaling data: %s", weatherWorkerType, ww.GetID(), err.Error())
			return err
		}

		err = ww.publishMessage(ctx, channel, dataAsBytes, "")
		if err != nil {
			log.Errorf("[worker: %s][workerID: %v][status: error][method: processData] error publishing message: %s", weatherWorkerType, ww.GetID(), err.Error())
			return err
		}
		return nil
	}

	log.Infof("[worker: %s][workerID: %v] INVALID DATA %s", weatherWorkerType, ww.GetID(), data)
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

func (ww *WeatherWorker) isValid(weatherData *weather.WeatherData) bool {
	return weatherData.Rainfall > ww.config.RainfallThreshold
}

func (ww *WeatherWorker) publishMessage(ctx context.Context, channel *amqp.Channel, data []byte, routingKey string) error {
	// At this moment (02/05/2023 1.20am) nasty things will begin
	targetExchange := exchangeOutput + weatherStr
	exchangeConfig, ok := ww.config.RabbitMQConfig[weatherStr][targetExchange]
	if !ok {
		return fmt.Errorf("[worker: %s][workerID: %v][status: error][method: publishMessage] invalid target exchange %s", weatherWorkerType, ww.GetID(), targetExchange)
	}

	publishConfig := exchangeConfig.PublishingConfig
	log.Infof("[worker: %s][workerID: %v][status: OK][method: publishMessage]Publishing message in exchange %s", weatherWorkerType, ww.GetID(), publishConfig.Exchange)

	// Generic idea for routing key: city.accumulatorName.brokerID. If we received a routing key, it means that is a wildcard routing key
	if routingKey == "" {
		routingKey = fmt.Sprintf("%s.rain-accumulator.%v", ww.config.City, ww.config.ID)
	}

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
