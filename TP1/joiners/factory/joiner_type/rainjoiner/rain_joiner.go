package rainjoiner

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
	"time"
	"tp1/communication"
	"tp1/domain/business/rainjoiner"
	"tp1/domain/entities/trip"
	"tp1/domain/entities/weather"
	"tp1/joiners/factory/joiner_type/rainjoiner/config"
	"tp1/utils"
)

const (
	rainJoinerType       = "rain-joiner"
	rainJoinerStr        = "rainjoiner"
	weatherStr           = "weather"
	tripsStr             = "trips"
	contentTypeJson      = "application/json"
	contentTypePlainText = "text/plain"
)

type RainJoiner struct {
	rabbitMQ *communication.RabbitMQ
	config   *config.RainJoinerConfig
	dateSet  utils.DateSet
	result   map[string]*rainjoiner.RainfallAccumulator
}

func NewRainJoiner(rabbitMQ *communication.RabbitMQ, config *config.RainJoinerConfig) *RainJoiner {
	dateSet := make(utils.DateSet)
	result := make(map[string]*rainjoiner.RainfallAccumulator)
	return &RainJoiner{
		rabbitMQ: rabbitMQ,
		config:   config,
		dateSet:  dateSet,
		result:   result,
	}
}

// GetID returns the Rain Joiner ID
func (rj *RainJoiner) GetID() string {
	return rj.config.ID
}

// GetType returns the Rain Joiner type
func (rj *RainJoiner) GetType() string {
	return rainJoinerType
}

// GetCity returns the Rain Joiner city
func (rj *RainJoiner) GetCity() string {
	return rj.config.City
}

func (rj *RainJoiner) getLogMessage(method string, message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[joiner: %s][city: %s][joinerID: %v][method:%s][status: ERROR] %s: %s", rainJoinerType, rj.GetCity(), rj.GetID(), method, message, err.Error())
	}
	return fmt.Sprintf("[joiner: %s][city: %s][joinerID: %s][method: %s][status: OK] %s", rainJoinerType, rj.GetCity(), rj.GetID(), method, message)
}

// GetRoutingKeys returns the Rain Joiner routing keys: rainjoiner.city.Qid and eof.rainjoiner.city
// OBS: this routing keys works fine for each input exchange
func (rj *RainJoiner) GetRoutingKeys() []string {
	return []string{
		fmt.Sprintf("%s.%s.%s", rainJoinerStr, rj.GetCity(), rj.GetID()), // input routing key: rainjoiner.city.Qid
		fmt.Sprintf("eof.weather.%s.%s", rainJoinerStr, rj.config.City),  // eof.weather.rainjoiner.city
		fmt.Sprintf("eof.trips.%s.%s", rainJoinerStr, rj.config.City),    // eof.trips.rainjoiner.city
	}
}

// GetEOFString returns the Rain Joiner EOF String.
func (rj *RainJoiner) GetEOFString() string {
	return fmt.Sprintf("eof.%s.%s", rainJoinerStr, rj.config.City)
}

// GetExpectedEOFString returns Rain Joiner's expected EOF
func (rj *RainJoiner) GetExpectedEOFString(data string) string {
	return fmt.Sprintf("eof.%s.%s", data, rj.config.City)
}

// DeclareQueues declares non-anonymous queues for Rain Joiner
// Queues: EOF queue, Rain Handler queue
func (rj *RainJoiner) DeclareQueues() error {
	err := rj.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{
		rj.config.EOFQueueConfig,
		rj.config.RainHandlerQueue,
	})
	if err != nil {
		return err
	}

	log.Info(rj.getLogMessage("DeclareQueues", "queues declared correctly!", nil))

	return nil
}

// DeclareExchanges declares exchanges for Rain Joiner
// Exchanges: weather-topic, weather-rainjoinner-topic
func (rj *RainJoiner) DeclareExchanges() error {
	var exchanges []communication.ExchangeDeclarationConfig
	for _, exchangeConfig := range rj.config.ExchangesConfig {
		exchanges = append(exchanges, exchangeConfig)
	}

	err := rj.rabbitMQ.DeclareExchanges(exchanges)
	if err != nil {
		return err
	}

	log.Infof(rj.getLogMessage("DeclareExchanges", "exchanges declared correctly!", nil))
	return nil
}

// JoinData joins the data from trips and weather. The flow of this function is:
// 1. Start consuming from the input exchange related with weather data
// 2. While we receive this data we save it in a map[date]*RainAccumulator
// 3. When we receive the message eof.weather.rainjoiner.city, we stop listening data about weather
// 4. Start consuming from the input exchange related with trips data
// 5. While we receive this data we perform a join with the map defined in step 2
// 6. When we receive the message eof.trips.rainjoiner.city, we stop listening data about trips
func (rj *RainJoiner) JoinData() error {
	err := rj.saveWeatherData()
	if err != nil {
		return err
	}
	log.Debug(rj.getLogMessage("JoinData", fmt.Sprintf("date set length: %v", len(rj.dateSet)), nil))

	err = rj.processTripData()
	if err != nil {
		return err
	}

	log.Debug(rj.getLogMessage("JoinData", "All data was joined successfully", nil))
	return nil
}

// SendResult summarizes the joined data and sends it to the Rain Handler
func (rj *RainJoiner) SendResult() error {
	var rainfallSummary *rainjoiner.RainfallAccumulator
	totalCount := 0
	var totalDuration float64

	for _, rainfallAccumulator := range rj.result {
		totalCount += rainfallAccumulator.Counter
		totalDuration += rainfallAccumulator.TotalDuration // because in a same date we can have multiple trips
	}

	rainfallSummary.SetCounter(totalCount)
	rainfallSummary.SetDuration(totalDuration)

	rainfallSummaryBytes, err := json.Marshal(rainfallSummary)
	if err != nil {
		log.Error(rj.getLogMessage("SendResult", "error marshalling data", ErrMarshallingSummary))
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = rj.rabbitMQ.PublishMessageInQueue(ctx, rj.config.RainHandlerQueue.Name, rainfallSummaryBytes, contentTypeJson)
	if err != nil {
		log.Error(rj.getLogMessage("SendResult", "error sending summary", err))
		return err
	}
	return nil
}

// SendEOF notifies the EOF Manager that the work of this joiner is done
func (rj *RainJoiner) SendEOF() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eofMessage := []byte(rj.GetEOFString())

	err := rj.rabbitMQ.PublishMessageInQueue(ctx, rj.config.EOFQueueConfig.Name, eofMessage, contentTypePlainText)
	if err != nil {
		log.Error(rj.getLogMessage("SendResult", fmt.Sprintf("error sending EOF message: %s", rj.GetEOFString()), err))
		return err
	}
	return nil
}

func (rj *RainJoiner) getConsumer(targetExchange string) (<-chan amqp091.Delivery, error) {
	exchangeName, ok := rj.config.InputExchanges[targetExchange]
	if !ok {
		log.Errorf(rj.getLogMessage("getConsumer", fmt.Sprintf("input exchange related with '%s' key not found", targetExchange), ErrExchangeNotFound))
	}
	routingKeys := rj.GetRoutingKeys()

	consumer, err := rj.rabbitMQ.GetExchangeConsumer(exchangeName, routingKeys)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

// saveWeatherData saves the date of the weather data that arrives to this joiner
func (rj *RainJoiner) saveWeatherData() error {
	consumer, err := rj.getConsumer(weatherStr)
	if err != nil {
		return err
	}

	log.Info(rj.getLogMessage("saveWeatherData", "start consuming weather messages", nil))
	eofWeatherString := rj.GetExpectedEOFString(weatherStr)

	for message := range consumer {
		// OBS: we use the benefit of Unmarshal. Here we can receive WeatherData or EOF, we know what type is based on the Metadata attribute
		var data weather.WeatherData
		err = json.Unmarshal(message.Body, &data)
		if err != nil {
			log.Error(rj.getLogMessage("saveWeatherData", "error unmarshalling data", ErrUnmarshallingWeatherData))
			return err
		}

		metadata := data.GetMetadata()

		if metadata.GetType() == rj.config.EOFType {
			// sanity checks
			if metadata.GetCity() != rj.GetCity() {
				panic(fmt.Sprintf("received an EOF message with of another city: Expected: %s - Got: %s", rj.GetCity(), metadata.GetCity()))
			}
			if metadata.GetMessage() != eofWeatherString {
				panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", eofWeatherString, metadata.GetMessage()))
			}

			log.Info(rj.getLogMessage("saveWeatherData", fmt.Sprintf("EOF received: %s", metadata.GetMessage()), nil))
			break
		}

		log.Debug(rj.getLogMessage("saveWeatherData", fmt.Sprintf("received weather data %+v", data), nil))
		rj.dateSet.Add(data.Date)
	}

	log.Info(rj.getLogMessage("saveWeatherData", "all weather data was saved!", nil))
	return nil
}

// processTripData performs the logic of the join operation. When a trip arrives, if it's date is in dateSet
// we update the
func (rj *RainJoiner) processTripData() error {
	consumer, err := rj.getConsumer(tripsStr)
	if err != nil {
		return err
	}

	log.Info(rj.getLogMessage("processTripData", "start consuming trips messages", nil))
	eofTripsString := rj.GetExpectedEOFString(tripsStr)

	for message := range consumer {
		var tripData trip.TripData
		err = json.Unmarshal(message.Body, &tripData)
		if err != nil {
			log.Error(rj.getLogMessage("saveWeatherData", "error unmarshalling data", ErrUnmarshallingTripData))
			return err
		}

		metadata := tripData.GetMetadata()

		if metadata.GetType() == rj.config.EOFType {
			// sanity checks
			if metadata.GetCity() != rj.GetCity() {
				panic(fmt.Sprintf("received an EOF message with of another city: Expected: %s - Got: %s", rj.GetCity(), metadata.GetCity()))
			}
			if metadata.GetMessage() != eofTripsString {
				panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", eofTripsString, metadata.GetMessage()))
			}

			log.Info(rj.getLogMessage("saveWeatherData", fmt.Sprintf("EOF received: %s", metadata.GetMessage()), nil))
			break
		}

		log.Debug(rj.getLogMessage("processTripData", fmt.Sprintf("received trip data %+v", tripData), nil))

		if rj.dateSet.Contains(tripData.StartDate) {
			key := tripData.StartDate.String()
			rainfallAccumulator, ok := rj.result[key]
			if !ok {
				newRainfallAccumulator := rainjoiner.NewRainfallAccumulator()
				newRainfallAccumulator.UpdateAccumulator(tripData.Duration)
				rj.result[key] = newRainfallAccumulator
				continue
			}

			rainfallAccumulator.UpdateAccumulator(tripData.Duration)
			rj.result[key] = rainfallAccumulator
		}
	}

	log.Info(rj.getLogMessage("processTripData", "all trip data was processed!", nil))
	return nil
}
