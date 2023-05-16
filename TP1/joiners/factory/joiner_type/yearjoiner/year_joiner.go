package yearjoiner

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
	"tp1/communication"
	"tp1/domain/business/tripcounter"
	"tp1/domain/entities"
	"tp1/domain/entities/eof"
	"tp1/domain/entities/station"
	"tp1/domain/entities/trip"
	"tp1/joiners/factory/joiner_type/yearjoiner/config"
	"tp1/utils"
)

const (
	yearJoinerType  = "year-joiner"
	yearJoinerStr   = "yearjoiner"
	tripCounterStr  = "trip-counter"
	stationsStr     = "stations"
	tripsStr        = "trips"
	contentTypeJson = "application/json"
	exchangeInput   = "exchange_input"
	year2016        = 2016
	year2017        = 2017
)

// YearJoiner
// + stationsMap: map with the following structure: {stationID-yearID: *station.StationData}
// + joinResult: map with following structure: {stationID1: {yearID1: TripCounter, yearID2: TripCounter}, stationIDN: {yearID1: TripCounter, yearID2: TripCounter}}
type YearJoiner struct {
	rabbitMQ    *communication.RabbitMQ
	config      *config.YearJoinerConfig
	stationsMap map[string]*station.StationData
	joinResult  map[string]map[int]*tripcounter.TripCounter
}

func NewYearJoiner(rabbitMQ *communication.RabbitMQ, yearJoinerConfig *config.YearJoinerConfig) *YearJoiner {
	stationsMap := make(map[string]*station.StationData)
	joinResult := make(map[string]map[int]*tripcounter.TripCounter) // the second map will have a capacity of 2
	return &YearJoiner{
		rabbitMQ:    rabbitMQ,
		config:      yearJoinerConfig,
		stationsMap: stationsMap,
		joinResult:  joinResult,
	}
}

func (yj *YearJoiner) getLogMessage(method string, message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[joiner: %s][city: %s][joinerID: %v][method:%s][status: ERROR] %s: %s", yearJoinerType, yj.GetCity(), yj.GetID(), method, message, err.Error())
	}
	return fmt.Sprintf("[joiner: %s][city: %s][joinerID: %s][method: %s][status: OK] %s", yearJoinerType, yj.GetCity(), yj.GetID(), method, message)
}

// GetID returns the Year Joiner ID. Possible values: Q1, Q2, Q3 or Q4
func (yj *YearJoiner) GetID() string {
	return yj.config.ID
}

// GetType returns the Year Joiner type
func (yj *YearJoiner) GetType() string {
	return yearJoinerType
}

func (yj *YearJoiner) GetCity() string {
	return yj.config.City
}

// GetRoutingKeys returns the Year Joiner routing keys: yearjoiner.city.Qid, yearjoiner.city.stations and eof.yearjoiner.city
// OBS: this routing keys works fine for each input exchange
func (yj *YearJoiner) GetRoutingKeys() []string {
	return []string{
		fmt.Sprintf("%s.%s.%s", yearJoinerStr, yj.GetCity(), yj.GetID()),  // input routing key: yearjoiner.city.Qid
		fmt.Sprintf("%s.%s.%s", yearJoinerStr, yj.GetCity(), stationsStr), // input routing key: yearjoiner.city.stations, special case due to stations do not have a Qid
		fmt.Sprintf("eof.%s.%s", yearJoinerStr, yj.GetCity()),             // eof.yearjoiner.city
	}
}

// GetEOFString eof of the YearJoiner
func (yj *YearJoiner) GetEOFString() string {
	return fmt.Sprintf("eof.%s.%s", yearJoinerStr, yj.GetCity())
}

// GetExpectedEOFString returns YearJoiner's expected EOF based on the data type
// Possible values: eof.stations.city or eof.trips.city
func (yj *YearJoiner) GetExpectedEOFString(dataType string) string {
	return fmt.Sprintf("eof.%s.%s", dataType, yj.GetCity())
}

// DeclareQueues declares non-anonymous queues for  YearJoiner
// Queues: EOF queue, Year Grouper 2016 and Year Grouper 2017 queue
func (yj *YearJoiner) DeclareQueues() error {
	err := yj.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{
		//cj.config.EOFQueueConfig,
		yj.config.YearGrouper2016Queue,
		yj.config.YearGrouper2017Queue,
	})

	if err != nil {
		return err
	}

	log.Info(yj.getLogMessage("DeclareQueues", "queues declared correctly!", nil))

	return nil
}

// DeclareExchanges declares exchanges for YearJoiner
// Exchanges: trips-yearjoiner-topic, stations-yearjoiner-topic
func (yj *YearJoiner) DeclareExchanges() error {
	var exchanges []communication.ExchangeDeclarationConfig
	var inputExchanges []string
	for key, exchangeConfig := range yj.config.ExchangesConfig {
		if strings.Contains(key, exchangeInput) {
			inputExchanges = append(inputExchanges, exchangeConfig.Name)
		}
		exchanges = append(exchanges, exchangeConfig)
	}

	err := yj.rabbitMQ.DeclareExchanges(exchanges)
	if err != nil {
		return err
	}

	// for input exchanges we need to perform the binding operation
	routingKeys := yj.GetRoutingKeys()
	err = yj.rabbitMQ.Bind(inputExchanges, routingKeys)
	if err != nil {
		return err
	}

	log.Infof(yj.getLogMessage("DeclareExchanges", "exchanges declared correctly!", nil))
	return nil
}

// JoinData joins the data from stations and trips. The flow of this function is:
// 1. Start consuming from the input exchange related with stations data
// 2. While we receive this data we save it in a map with the following structure: map[string]*station.StationData
// 3. When we receive the message eof.stations.city, we stop listening data about stations. The city in EOF must match the city to join at this stage
// 4. Start consuming from the input exchange related with trips data
// 5. While we receive this data we perform a join with the map defined in step 2
// 6. When we receive the message eof.trips.city, we stop listening data about trips
func (yj *YearJoiner) JoinData() error {
	err := yj.saveStationsData()
	if err != nil {
		return err
	}

	err = yj.processTripData()
	if err != nil {
		return err
	}

	log.Debug(yj.getLogMessage("JoinData", "All data was joined successfully", nil))
	return nil
}

// SendResult summarizes the data from 2016 and 2017 and sends it to the corresponding queues
func (yj *YearJoiner) SendResult() error {
	var data2016 []*tripcounter.TripCounter
	var data2017 []*tripcounter.TripCounter

	metadata2016 := entities.NewMetadata(
		yj.GetCity(),
		tripCounterStr,
		yearJoinerType,
		"",
	)

	metadata2017 := entities.NewMetadata(
		yj.GetCity(),
		tripCounterStr,
		yearJoinerType,
		"",
	)

	for _, tripCounterMap := range yj.joinResult {
		counter2016 := tripCounterMap[year2016]
		counter2016.Metadata = metadata2016
		data2016 = append(data2016, counter2016)

		counter2017 := tripCounterMap[year2017]
		counter2017.Metadata = metadata2017
		data2017 = append(data2016, counter2017)
	}

	data2016Bytes, err := json.Marshal(data2016)
	if err != nil {
		log.Error(yj.getLogMessage("SendResult", "error marshalling data2016 to send", err))
	}

	data2017Bytes, err := json.Marshal(data2017)
	if err != nil {
		log.Error(yj.getLogMessage("SendResult", "error marshalling data2017 to send", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = yj.rabbitMQ.PublishMessageInQueue(ctx, yj.config.YearGrouper2016Queue.Name, data2016Bytes, contentTypeJson)
	if err != nil {
		log.Error(yj.getLogMessage("SendResult", "error sending data2016", err))
		return err
	}

	err = yj.rabbitMQ.PublishMessageInQueue(ctx, yj.config.YearGrouper2017Queue.Name, data2017Bytes, contentTypeJson)
	if err != nil {
		log.Error(yj.getLogMessage("SendResult", "error sending data2017", err))
		return err
	}

	return nil
}

// SendEOF notifies the EOF Manager that the work of YearJoiner is done
func (yj *YearJoiner) SendEOF() error {
	eofData := eof.NewEOF(yj.GetCity(), yearJoinerType, yj.GetEOFString())
	eofDataBytes, err := json.Marshal(eofData)
	if err != nil {
		return fmt.Errorf("%w: error marshalling EOF message: %s", err, err.Error())
	}
	eofQueueName := yj.config.EOFQueueConfig.Name

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = yj.rabbitMQ.PublishMessageInQueue(ctx, eofQueueName, eofDataBytes, contentTypeJson)
	if err != nil {
		log.Error(yj.getLogMessage("SendEOF", fmt.Sprintf("error sending EOF message: %s", yj.GetEOFString()), err))
		return err
	}
	return nil
}

func (yj *YearJoiner) Kill() error {
	return yj.rabbitMQ.KillBadBunny()
}

// getExchangeNameForDataType returns the input exchange name based on the data type.
// Possible values for dataType: stations, trips
func (yj *YearJoiner) getExchangeNameForDataType(dataType string) (string, error) {
	exchangeName, ok := yj.config.InputExchanges[dataType]
	if !ok {
		return "", fmt.Errorf("input exchange related with '%s' key not found", dataType)
	}
	return exchangeName, nil
}

// saveStationsData saves the stations data in memory. The key of the map is stationCode-yearID
func (yj *YearJoiner) saveStationsData() error {
	exchangeName, err := yj.getExchangeNameForDataType(stationsStr)
	if err != nil {
		return err
	}

	consumer, err := yj.rabbitMQ.GetConsumerForExchange(exchangeName)
	if err != nil {
		return err
	}

	log.Info(yj.getLogMessage("saveStationsData", "start consuming stations messages", nil))
	eofStationString := yj.GetExpectedEOFString(stationsStr)

outerStationsLoop:
	for message := range consumer {
		var stationsData []*station.StationData
		err = json.Unmarshal(message.Body, &stationsData)
		if err != nil {
			log.Error(yj.getLogMessage("saveStationsData", "error unmarshalling data", err))
			return err
		}

		// analyze each station
		for idx := range stationsData {
			stationData := stationsData[idx]
			metadata := stationData.GetMetadata()

			// sanity check
			if metadata.GetCity() != yj.GetCity() {
				panic(fmt.Sprintf("received a message with another city: Expected: %s - Got: %s", yj.GetCity(), metadata.GetCity()))
			}

			if metadata.GetType() == yj.config.EOFType {
				// sanity-check
				if metadata.GetMessage() != eofStationString {
					panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", eofStationString, metadata.GetMessage()))
				}

				log.Info(yj.getLogMessage("saveStationsData", fmt.Sprintf("EOF received: %s", eofStationString), nil))
				break outerStationsLoop
			}

			// sanity check
			if !utils.ContainsInt(stationData.YearID, yj.config.ValidYears) {
				panic(fmt.Sprintf("received an invalid year. Expected years are: %v. Got: %v", yj.config.ValidYears, stationData.YearID))
			}

			mapKey := stationData.GetPrimaryKey()
			_, ok := yj.stationsMap[mapKey]
			if !ok {
				yj.stationsMap[mapKey] = stationData
			}
		}
	}

	log.Info(yj.getLogMessage("saveStationsData", "all station data was saved!", nil))
	return nil
}

// processTripData get trips data and perform a join operation based on certain conditions with the station data
// that was saved previously
func (yj *YearJoiner) processTripData() error {
	exchangeName, err := yj.getExchangeNameForDataType(tripsStr)
	if err != nil {
		return err
	}

	consumer, err := yj.rabbitMQ.GetConsumerForExchange(exchangeName)
	if err != nil {
		return err
	}

	log.Info(yj.getLogMessage("processTripData", "start consuming trip messages", nil))
	eofTripsString := yj.GetExpectedEOFString(tripsStr)

outerTripsLoop:
	for message := range consumer {
		var tripsData []*trip.TripData
		err = json.Unmarshal(message.Body, &tripsData)
		if err != nil {
			log.Error(yj.getLogMessage("processTripData", "error unmarshalling data", err))
			return err
		}

		for idx := range tripsData {
			tripData := tripsData[idx]
			metadata := tripData.GetMetadata()

			// sanity check
			if metadata.GetCity() != yj.GetCity() {
				panic(fmt.Sprintf("received a message with another city: Expected: %s - Got: %s", yj.GetCity(), metadata.GetCity()))
			}

			if metadata.GetType() == yj.config.EOFType {
				// sanity check
				if metadata.GetMessage() != eofTripsString {
					panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", eofTripsString, metadata.GetMessage()))
				}

				log.Info(yj.getLogMessage("processTripData", fmt.Sprintf("EOF received: %s", eofTripsString), nil))
				break outerTripsLoop
			}

			// sanity check
			if !utils.ContainsInt(tripData.YearID, yj.config.ValidYears) {
				panic(fmt.Sprintf("received an invalid trip yearID. Expected years are: %v. Got: %v", yj.config.ValidYears, tripData.YearID))
			}

			// check if we have the data of the station, otherwise analyze the next trip data
			stationKey := getStationsMapKey(tripData.StartStationCode, tripData.YearID)
			stationData, ok := yj.stationsMap[stationKey]
			if !ok {
				log.Debug(yj.getLogMessage("processTripData", fmt.Sprintf("dont have data of station %s, AKA it was not used in %v", stationKey, tripData.YearID), nil))
				continue
			}
			stationIDStr := strconv.Itoa(tripData.StartStationCode)

			// check if we already have data about this station
			stationDataCounters, ok := yj.joinResult[stationIDStr]
			if !ok {
				// We dont have data, we initialize it
				newCounterMap := yj.initializeCounterMap(stationData)
				yearCounter := newCounterMap[tripData.YearID]
				yearCounter.UpdateCounter()
				newCounterMap[tripData.YearID] = yearCounter
				yj.joinResult[stationIDStr] = newCounterMap
				continue
			}

			yearCounter := stationDataCounters[tripData.YearID]
			yearCounter.UpdateCounter()
			stationDataCounters[tripData.YearID] = yearCounter
			yj.joinResult[stationIDStr] = stationDataCounters
		}
	}

	log.Info(yj.getLogMessage("processTripData", "all trip data was processed!", nil))
	return nil
}

func (yj *YearJoiner) initializeCounterMap(stationData *station.StationData) map[int]*tripcounter.TripCounter {
	counterMap := make(map[int]*tripcounter.TripCounter, len(yj.config.ValidYears)) // Limited capacity
	counter2016 := tripcounter.NewTripCounter(stationData.Name, stationData.Code, year2016)
	counter2017 := tripcounter.NewTripCounter(stationData.Name, stationData.Code, year2017)

	counterMap[year2016] = counter2016
	counterMap[year2017] = counter2017

	return counterMap
}

// getStationsMapKey returns the key for the YearJoiner.stationsMap. The key structure is stationCode-yearID
func getStationsMapKey(stationCode int, yearID int) string {
	return fmt.Sprintf("%v-%v", stationCode, yearID)
}
