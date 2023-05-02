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
	dataErrors "tp1/brokers/errors"
	"tp1/brokers/weather/config"
	"tp1/domain/entities/weather"
)

const (
	dateLayout     = "2006-01-02"
	rabbitUrl      = "amqp://guest:guest@rabbit:5672/"
	brokerType     = "Weather Broker"
	weatherStr     = "weather"
	exchangeInput  = "exchange_input_"
	exchangeOutput = "exchange_output_"
)

type WeatherBroker struct {
	config    *config.WeatherConfig
	delimiter string
}

func NewWeatherBroker(delimiter string, weatherBrokerConfig *config.WeatherConfig) *WeatherBroker {
	return &WeatherBroker{
		delimiter: delimiter,
		config:    weatherBrokerConfig,
	}
}

// DeclareQueues Declares non anonymous queues
func (wb *WeatherBroker) DeclareQueues() error {
	conn, err := amqp.Dial(rabbitUrl)
	if err != nil {
		log.Errorf("[broker: %s][status: error] failed to connect to RabbitMQ: %s", brokerType, err.Error())
		return err
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Errorf("[broker: %s][status: error] failed to open a channel: %s", brokerType, err.Error())
		return err
	}

	defer ch.Close()

	aux, ok := wb.config.RabbitMQConfig[weatherStr]
	rabbitQueueConfig, ok := aux["exchange_input_weather"]
	if !ok {
		log.Errorf("[broker: %s][status: error] config does not exists: %s", brokerType, err.Error())
		return err
	}

	_, err = ch.QueueDeclare(
		rabbitQueueConfig.QueueDeclarationConfig.Name,
		rabbitQueueConfig.QueueDeclarationConfig.Durable,
		rabbitQueueConfig.QueueDeclarationConfig.DeleteWhenUnused,
		rabbitQueueConfig.QueueDeclarationConfig.Exclusive,
		rabbitQueueConfig.QueueDeclarationConfig.NoWait,
		nil,
	)

	if err != nil {
		log.Errorf("[broker: %s][status: error] error declaring queue %s: %s", brokerType, err.Error())
		return err
	}

	log.Infof("[broker: %s][status: OK] queue declared correctly %s!", brokerType, rabbitQueueConfig.QueueDeclarationConfig.Name)

	return nil
}

// DeclareExchanges Declares exchanges
func (wb *WeatherBroker) DeclareExchanges() error {
	conn, err := amqp.Dial(rabbitUrl)
	if err != nil {
		log.Errorf("[broker: %s][status: error] failed to connect to RabbitMQ: %s", brokerType, err.Error())
		return err
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Errorf("[broker: %s][status: error] failed to open a channel: %s", brokerType, err.Error())
		return err
	}

	defer ch.Close()

	for key, rabbitConfig := range wb.config.RabbitMQConfig[weatherStr] {
		if strings.Contains(key, "exchange") {
			err = ch.ExchangeDeclare(
				rabbitConfig.ExchangeDeclarationConfig.Name,
				rabbitConfig.ExchangeDeclarationConfig.Type,
				rabbitConfig.ExchangeDeclarationConfig.Durable,
				rabbitConfig.ExchangeDeclarationConfig.AutoDeleted,
				rabbitConfig.ExchangeDeclarationConfig.Internal,
				rabbitConfig.ExchangeDeclarationConfig.NoWait,
				nil,
			)
			if err != nil {
				log.Errorf("[broker: %s][status: error] error declaring exchange %s: %s", brokerType, rabbitConfig.ExchangeDeclarationConfig.Name, err.Error())
				return err
			}
		}
	}

	return nil
}

// ProcessInputMessages process messages that arrives in certain topic related with the broker
func (wb *WeatherBroker) ProcessInputMessages() error {
	conn, err := amqp.Dial(rabbitUrl)
	if err != nil {
		log.Errorf("[broker: %s][status: error][method: ProcessInputMessages] failed to connect to RabbitMQ: %s", brokerType, err.Error())
		return err
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Errorf("[broker: %s][status: error][method: ProcessInputMessages] failed to open a channel: %s", brokerType, err.Error())
		return err
	}

	defer ch.Close()

	configKey := exchangeInput + weatherStr
	rabbitMQ, ok := wb.config.RabbitMQConfig[weatherStr][configKey]
	if !ok {
		log.Errorf("[broker: %s][status: error][method: ProcessInputMessages] cannot find RabbitMQ config for key %s: %s", brokerType, configKey, err.Error())
		return err
	}

	anonymousQueue, err := ch.QueueDeclare(
		"",
		rabbitMQ.QueueDeclarationConfig.Durable,
		rabbitMQ.QueueDeclarationConfig.DeleteWhenUnused,
		rabbitMQ.QueueDeclarationConfig.Exclusive,
		rabbitMQ.QueueDeclarationConfig.NoWait,
		nil,
	)

	if err != nil {
		log.Errorf("[broker: %s][status: error][method: ProcessInputMessages] error declaring anonymus queue: %s", brokerType, err.Error())
		return err
	}

	// Binding
	inputRoutingKey := fmt.Sprintf("%s.%s.%v", weatherStr, wb.config.City, wb.config.ID)
	eofRoutingKey := fmt.Sprintf("%s.eof", weatherStr)
	routingKeys := []string{inputRoutingKey, eofRoutingKey}

	for _, routingKey := range routingKeys {
		err = ch.QueueBind(
			anonymousQueue.Name,
			routingKey,
			rabbitMQ.ExchangeDeclarationConfig.Name,
			rabbitMQ.ExchangeDeclarationConfig.NoWait,
			nil,
		)
		if err != nil {
			log.Errorf("[broker: %s][status: error][method: ProcessInputMessages] error binding routing key %s: %s", brokerType, routingKey, err.Error())
			return err
		}
	}

	messages, err := ch.Consume(
		anonymousQueue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Errorf("[broker: %s][status: error][method: ProcessInputMessages] error getting consumer: %s", brokerType, err.Error())
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Debugf("[broker: %s][status: OK][method: ProcessInputMessages] start consuming messages", brokerType)
	eofString := weatherStr + "-" + "PONG"
	for d := range messages {
		sms := string(d.Body)
		if sms == eofString {
			log.Infof("[ID: %v][broker: %s][status: OK][method: ProcessInputMessages] EOF received", wb.config.ID, brokerType)
			break
		}
		log.Debugf("[broker: %s][status: OK][method: ProcessInputMessages] received message %s", brokerType, sms)
		smsSplit := strings.Split(sms, "|")
		for _, smsData := range smsSplit {
			if strings.Contains(smsData, "PING") {
				log.Debug("bypassing PING")
				continue
			}

			err = wb.processData(ctx, ch, smsData)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// processData receives data about ONLY ONE weather and process it
func (wb *WeatherBroker) processData(ctx context.Context, channel *amqp.Channel, data string) error {
	log.Infof("Processing WEATHER data: %s", data)
	weatherData, err := wb.getWeatherData(data)
	if err != nil {
		if errors.Is(err, dataErrors.ErrInvalidWeatherData) {
			return nil
		}
		return err
	}

	if wb.isValid(weatherData) {
		log.Infof("[broker: %s][status: OK][method: processData]: valid weather data %+v", brokerType, weatherData)
		weatherData.City = wb.config.City
		weatherData.Type = weatherStr
		err = wb.publishMessage(ctx, channel, weatherData)
		if err != nil {
			log.Errorf("[broker: %s][status: error][method: processData] error publishing message: %s", brokerType, err.Error())
			return err
		}
		return nil
	}

	log.Infof("INVALID DATA %s", data)
	return nil
}

// getWeatherData returns a struct WeatherData
func (wb *WeatherBroker) getWeatherData(data string) (*weather.WeatherData, error) {
	dataSplit := strings.Split(data, wb.delimiter)
	date, err := time.Parse(dateLayout, dataSplit[wb.config.ValidColumnsIndexes.Date])
	if err != nil {
		log.Debugf("Invalid date %s", dataSplit[wb.config.ValidColumnsIndexes.Date])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidDate, dataErrors.ErrInvalidWeatherData)
	}

	date = date.AddDate(0, 0, -1)

	rainfall, err := strconv.ParseFloat(dataSplit[wb.config.ValidColumnsIndexes.Rainfall], 64)
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
func (wb *WeatherBroker) isValid(weatherData *weather.WeatherData) bool {
	return weatherData.Rainfall > wb.config.RainfallThreshold
}

func (wb *WeatherBroker) publishMessage(ctx context.Context, channel *amqp.Channel, weatherData *weather.WeatherData) error {
	// At this moment (02/05/2023 1.20am) nasty things will begin
	exchangeConfig, ok := wb.config.RabbitMQConfig[weatherStr][exchangeOutput+weatherStr]
	if !ok {
		return fmt.Errorf("[broker: %s][status: error][method: publishMessage] invalid target exchange %s", brokerType, exchangeOutput+weatherStr)
	}

	publishConfig := exchangeConfig.PublishingConfig
	log.Infof("[broker: %s][status: OK][method: publishMessage]Publishing message in exchange %s", brokerType, publishConfig.Exchange)

	dataAsBytes, err := json.Marshal(weatherData)
	if err != nil {
		log.Errorf("[broker: %s][status: error][method: publishMessage] error marshaling data: %s", brokerType, err.Error())
		return err
	}

	return channel.PublishWithContext(ctx,
		publishConfig.Exchange,
		"rain_accumulator.1", // ToDo: replace with a valid string. Licha
		publishConfig.Mandatory,
		publishConfig.Immediate,
		amqp.Publishing{
			ContentType: publishConfig.ContentType,
			Body:        dataAsBytes,
		},
	)
}
