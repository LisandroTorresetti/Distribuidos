package cityjoiner

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/umahmood/haversine"
	"strconv"
	"strings"
	"time"
	"tp1/communication"
	"tp1/domain/business/distanceaccumulator"
	"tp1/domain/entities"
	"tp1/domain/entities/eof"
	"tp1/domain/entities/station"
	"tp1/domain/entities/trip"
	"tp1/joiners/factory/joiner_type/cityjoiner/config"
	"tp1/utils"
)

const (
	cityJoinerType         = "city-joiner"
	cityJoinerStr          = "cityjoiner"
	distanceAccumulatorStr = "distance-accumulator"
	stationsStr            = "stations"
	tripsStr               = "trips"
	contentTypeJson        = "application/json"
	exchangeInput          = "exchange_input"
)

type CityJoiner struct {
	rabbitMQ    *communication.RabbitMQ
	config      *config.CityJoinerConfig
	stationsMap map[string]*station.StationData
	joinResult  map[string]*distanceaccumulator.DistanceAccumulator
}

func NewCityJoiner(rabbitMQ *communication.RabbitMQ, cityJoinerConfig *config.CityJoinerConfig) *CityJoiner {
	stationsMap := make(map[string]*station.StationData)
	joinResult := make(map[string]*distanceaccumulator.DistanceAccumulator)
	return &CityJoiner{
		rabbitMQ:    rabbitMQ,
		config:      cityJoinerConfig,
		stationsMap: stationsMap,
		joinResult:  joinResult,
	}
}

func (cj *CityJoiner) getLogMessage(method string, message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[joiner: %s][city: %s][joinerID: %v][method:%s][status: ERROR] %s: %s", cityJoinerType, cj.GetCity(), cj.GetID(), method, message, err.Error())
	}
	return fmt.Sprintf("[joiner: %s][city: %s][joinerID: %s][method: %s][status: OK] %s", cityJoinerType, cj.GetCity(), cj.GetID(), method, message)
}

// GetID returns the City Joiner ID
func (cj *CityJoiner) GetID() string {
	return cj.config.ID
}

// GetType returns the City Joiner type
func (cj *CityJoiner) GetType() string {
	return cityJoinerType
}

func (cj *CityJoiner) GetCity() string {
	return cj.config.City
}

// GetRoutingKeys returns the City Joiner routing keys: cityjoiner.city.Qid and eof.cityjoiner.city
// OBS: this routing keys works fine for each input exchange
func (cj *CityJoiner) GetRoutingKeys() []string {
	return []string{
		fmt.Sprintf("%s.%s.%s", cityJoinerStr, cj.GetCity(), cj.GetID()),  // input routing key: cityjoiner.city.Qid
		fmt.Sprintf("%s.%s.%s", cityJoinerStr, cj.GetCity(), stationsStr), // input routing key: cityjoiner.city.stations, special case due to stations do not have a Qid
		fmt.Sprintf("eof.%s.%s", cityJoinerStr, cj.config.City),           // eof.cityjoiner.city
	}
}

// GetEOFString eof of the CityJoiner
func (cj *CityJoiner) GetEOFString() string {
	return fmt.Sprintf("eof.%s.%s", cityJoinerStr, cj.GetCity())
}

// GetExpectedEOFString returns City Joiner's expected EOF based on the data type
func (cj *CityJoiner) GetExpectedEOFString(dataType string) string {
	return fmt.Sprintf("eof.%s.%s", dataType, cj.GetCity())
}

// DeclareQueues declares non-anonymous queues for City Joiner
// Queues: EOF queue, City Handler queue
func (cj *CityJoiner) DeclareQueues() error {
	err := cj.rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{
		//cj.config.EOFQueueConfig,
		cj.config.CityHandlerQueue,
	})

	if err != nil {
		return err
	}

	log.Info(cj.getLogMessage("DeclareQueues", "queues declared correctly!", nil))

	return nil
}

// DeclareExchanges declares exchanges for City Joiner
// Exchanges: trips-cityjoiner-topic, stations-cityjoiner-topic
func (cj *CityJoiner) DeclareExchanges() error {
	var exchanges []communication.ExchangeDeclarationConfig
	var inputExchanges []string
	for key, exchangeConfig := range cj.config.ExchangesConfig {
		if strings.Contains(key, exchangeInput) {
			inputExchanges = append(inputExchanges, exchangeConfig.Name)
		}
		exchanges = append(exchanges, exchangeConfig)
	}

	err := cj.rabbitMQ.DeclareExchanges(exchanges)
	if err != nil {
		return err
	}

	// for input exchanges we need to perform the binding operation
	routingKeys := cj.GetRoutingKeys()
	err = cj.rabbitMQ.Bind(inputExchanges, routingKeys)
	if err != nil {
		return err
	}

	log.Infof(cj.getLogMessage("DeclareExchanges", "exchanges declared correctly!", nil))
	return nil
}

// JoinData joins the data from stations and trips. The flow of this function is:
// 1. Start consuming from the input exchange related with stations data
// 2. While we receive this data we save it in a map[string]*station.StationData, the key is stationID-yearID, doing this we know when a station was used
// 3. When we receive the message eof.stations.city, we stop listening data about stations. The city in EOF must match the city to join at this stage
// 4. Start consuming from the input exchange related with trips data
// 5. While we receive this data we perform a join with the map defined in step 2. The map of this step has keys equal to the stationID (NOT stationID-yearID)
// 6. When we receive the message eof.trips.city, we stop listening data about trips
func (cj *CityJoiner) JoinData() error {
	err := cj.saveStationsData()
	if err != nil {
		return err
	}

	err = cj.processTripData()
	if err != nil {
		return err
	}

	log.Debug(cj.getLogMessage("JoinData", "All data was joined successfully", nil))
	return nil
}

// SendResult summarizes the joined data and sends it to the City Handler
func (cj *CityJoiner) SendResult() error {
	var joinedDataSlice []*distanceaccumulator.DistanceAccumulator

	metadata := entities.NewMetadata(
		cj.GetCity(),
		distanceAccumulatorStr,
		cityJoinerType,
		"",
	)

	for _, accumulator := range cj.joinResult {
		accumulator.Metadata = metadata
		joinedDataSlice = append(joinedDataSlice, accumulator)
	}

	validDataBytes, err := json.Marshal(joinedDataSlice)
	if err != nil {
		log.Error(cj.getLogMessage("SendResult", "error marshalling valid data to send", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = cj.rabbitMQ.PublishMessageInQueue(ctx, cj.config.CityHandlerQueue.Name, validDataBytes, contentTypeJson)
	if err != nil {
		log.Error(cj.getLogMessage("SendResult", "error sending summary", err))
		return err
	}

	return nil
}

// SendEOF notifies the EOF Manager that the work of this joiner is done
func (cj *CityJoiner) SendEOF() error {
	eofData := eof.NewEOF(cj.GetCity(), cityJoinerType, cj.GetEOFString())
	eofDataBytes, err := json.Marshal(eofData)
	if err != nil {
		return fmt.Errorf("%w: error marshalling EOF message: %s", err, err.Error())
	}
	eofQueueName := cj.config.EOFQueueConfig.Name

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = cj.rabbitMQ.PublishMessageInQueue(ctx, eofQueueName, eofDataBytes, contentTypeJson)
	if err != nil {
		log.Error(cj.getLogMessage("SendEOF", fmt.Sprintf("error sending EOF message: %s", cj.GetEOFString()), err))
		return err
	}
	return nil
}

func (cj *CityJoiner) Kill() error {
	return cj.rabbitMQ.KillBadBunny()
}

// saveStationsData saves the stations data in memory. The key of the map is stationCode-yearID
func (cj *CityJoiner) saveStationsData() error {
	exchangeName, err := cj.getExchangeNameForDataType(stationsStr)
	if err != nil {
		return err
	}

	consumer, err := cj.rabbitMQ.GetConsumerForExchange(exchangeName)
	if err != nil {
		return err
	}

	log.Info(cj.getLogMessage("saveStationsData", "start consuming stations messages", nil))
	eofStationString := cj.GetExpectedEOFString(stationsStr)

outerStationsLoop:
	for message := range consumer {
		var stationsData []*station.StationData
		err = json.Unmarshal(message.Body, &stationsData)
		if err != nil {
			log.Error(cj.getLogMessage("saveStationsData", "error unmarshalling data", ErrUnmarshallingStationData))
			return err
		}

		// analyze each station
		for idx := range stationsData {
			stationData := stationsData[idx]

			// sanity check: there are stations that does not have latitude or longitude set, we skip them
			if !stationData.HasValidCoordinates() {
				continue
			}

			metadata := stationData.GetMetadata()

			// sanity check
			if !utils.ContainsString(metadata.GetCity(), cj.config.ValidCities) {
				panic(fmt.Sprintf("Valid cities are %v, got: %s", cj.config.ValidCities, metadata.GetCity()))
			}

			if metadata.GetType() == cj.config.EOFType {
				// sanity checks
				if metadata.GetCity() != cj.GetCity() {
					panic(fmt.Sprintf("received an EOF message with another city: Expected: %s - Got: %s", cj.GetCity(), metadata.GetCity()))
				}
				if metadata.GetMessage() != eofStationString {
					panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", eofStationString, metadata.GetMessage()))
				}

				log.Info(cj.getLogMessage("saveStationsData", fmt.Sprintf("EOF received: %s", eofStationString), nil))
				break outerStationsLoop
			}

			mapKey := stationData.GetPrimaryKey()
			_, ok := cj.stationsMap[mapKey]
			if !ok {
				cj.stationsMap[mapKey] = stationData // saves if the station was used in some year
			}
		}
	}

	log.Info(cj.getLogMessage("saveStationsData", "all station data was saved!", nil))
	return nil
}

// processTripData get trips data and perform a join operation based on certain conditions with the station data
// that was saved previously
func (cj *CityJoiner) processTripData() error {
	exchangeName, err := cj.getExchangeNameForDataType(tripsStr)
	if err != nil {
		return err
	}

	consumer, err := cj.rabbitMQ.GetConsumerForExchange(exchangeName)
	if err != nil {
		return err
	}

	log.Info(cj.getLogMessage("processTripData", "start consuming trip messages", nil))
	eofTripsString := cj.GetExpectedEOFString(tripsStr)

outerTripsLoop:
	for message := range consumer {
		var tripsData []*trip.TripData
		err = json.Unmarshal(message.Body, &tripsData)
		if err != nil {
			log.Error(cj.getLogMessage("processTripData", "error unmarshalling data", ErrUnmarshallingTripsData))
			return err
		}

		for idx := range tripsData {
			tripData := tripsData[idx]
			metadata := tripData.GetMetadata()

			// sanity check
			if !utils.ContainsString(metadata.GetCity(), cj.config.ValidCities) {
				panic(fmt.Sprintf("Valid cities are %v, got: %s", cj.config.ValidCities, metadata.GetCity()))
			}

			if metadata.GetType() == cj.config.EOFType {
				// sanity checks
				if metadata.GetCity() != cj.GetCity() {
					panic(fmt.Sprintf("received an EOF message with another city: Expected: %s - Got: %s", cj.GetCity(), metadata.GetCity()))
				}
				if metadata.GetMessage() != eofTripsString {
					panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", eofTripsString, metadata.GetMessage()))
				}

				log.Info(cj.getLogMessage("processTripData", fmt.Sprintf("EOF received: %s", eofTripsString), nil))
				break outerTripsLoop
			}

			// check if we have the data of both start station and end station
			if !cj.haveDataOfBothStations(tripData.StartStationCode, tripData.EndStationCode, tripData.YearID) {
				continue
			}

			startStationKey := getStationsMapKey(tripData.StartStationCode, tripData.YearID)
			endStationKey := getStationsMapKey(tripData.EndStationCode, tripData.YearID)

			startStation := cj.stationsMap[startStationKey]
			endStation := cj.stationsMap[endStationKey]

			distance := calculateDistance(startStation, endStation)

			endStationIDStr := strconv.Itoa(tripData.EndStationCode)
			distanceAccumulator, ok := cj.joinResult[endStationIDStr] // We need to save the ID only, because the same station is used in different years
			if !ok {
				// the data about the end stations wasn't in joinResult, we have to add it
				newDistanceAccumulator := distanceaccumulator.NewDistanceAccumulator(endStation.Name, endStationIDStr)
				newDistanceAccumulator.UpdateAccumulator(distance)
				cj.joinResult[endStationIDStr] = newDistanceAccumulator
				continue
			}
			distanceAccumulator.UpdateAccumulator(distance)
			cj.joinResult[endStationIDStr] = distanceAccumulator
		}
	}

	log.Info(cj.getLogMessage("processTripData", "all trip data was processed!", nil))
	return nil
}

// getExchangeNameForDataType returns the input exchange name based on the data type.
// Possible values for dataType: stations, trips
func (cj *CityJoiner) getExchangeNameForDataType(dataType string) (string, error) {
	exchangeName, ok := cj.config.InputExchanges[dataType]
	if !ok {
		return "", fmt.Errorf("input exchange related with '%s' key not found", dataType)
	}
	return exchangeName, nil
}

// haveDataOfBothStations returns true if both stations are in CityJoiner.stationsMap
func (cj *CityJoiner) haveDataOfBothStations(startStationCode int, endStationCode int, yearID int) bool {
	startStationKey := getStationsMapKey(startStationCode, yearID)
	_, ok := cj.stationsMap[startStationKey]
	if !ok {
		return false
	}

	endStationKey := getStationsMapKey(endStationCode, yearID)
	_, ok = cj.stationsMap[endStationKey]

	return ok
}

// getStationsMapKey returns the key for the CityJoiner.stationsMap. The key structure is stationCode-yearID
func getStationsMapKey(stationCode int, yearID int) string {
	return fmt.Sprintf("%v-%v", stationCode, yearID)
}

// calculateDistance returns the distance between two stations using haversine formula
func calculateDistance(startStation *station.StationData, endStation *station.StationData) float64 {
	latStartStation, longStartStation := startStation.GetCoordinates()
	latEndStation, longEndStation := endStation.GetCoordinates()
	station1 := haversine.Coord{Lat: latStartStation, Lon: longStartStation}
	station2 := haversine.Coord{Lat: latEndStation, Lon: longEndStation}

	_, km := haversine.Distance(station1, station2)
	return km
}
