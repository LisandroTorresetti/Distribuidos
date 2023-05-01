package trip

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
	dataErrors "tp1/brokers/errors"
	"tp1/brokers/trip/config"
	"tp1/domain/entities/trip"
)

const dateLayout = "2023-04-22"

type TripBroker struct {
	config    *config.TripConfig
	delimiter string
}

func NewTripBroker(delimiter string, tripBrokerConfig *config.TripConfig) *TripBroker {
	return &TripBroker{
		delimiter: delimiter,
		config:    tripBrokerConfig,
	}
}

func (tb *TripBroker) ProcessData(data string) error {
	log.Infof("Processing TRIP data: %s", data)
	tripData, err := tb.getTripData(data)
	if err != nil {
		if errors.Is(err, dataErrors.ErrInvalidTripData) {
			return nil
		}
		return err
	}

	if tb.isValid(tripData) {
		// ToDo: here we should send a message to rabbitMQ
		log.Infof("IS VALID %s", data)
		return nil
	}

	log.Infof("INVALID DATA %s", data)
	return nil
}

// getTripData returns a struct TripData
func (tb *TripBroker) getTripData(data string) (*trip.TripData, error) {
	dataSplit := strings.Split(data, tb.delimiter)
	startDate, err := time.Parse(dateLayout, dataSplit[tb.config.ValidColumnsIndexes.StartDate])
	if err != nil {
		log.Debugf("Invalid start date: %v", dataSplit[tb.config.ValidColumnsIndexes.StartDate])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidDate, dataErrors.ErrInvalidTripData)
	}

	endDate, err := time.Parse(dateLayout, dataSplit[tb.config.ValidColumnsIndexes.EndDate])
	if err != nil {
		log.Debugf("Invalid end date; %v", dataSplit[tb.config.ValidColumnsIndexes.EndDate])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidDate, dataErrors.ErrInvalidTripData)
	}

	startStationCodeID, err := strconv.Atoi(dataSplit[tb.config.ValidColumnsIndexes.StartStationCode])
	if err != nil {
		log.Debugf("Invalid start station code ID: %v", dataSplit[tb.config.ValidColumnsIndexes.StartStationCode])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrStationCodeType, dataErrors.ErrInvalidTripData)
	}

	endStationCodeID, err := strconv.Atoi(dataSplit[tb.config.ValidColumnsIndexes.EndStationCode])
	if err != nil {
		log.Debugf("Invalid end station code ID: %v", dataSplit[tb.config.ValidColumnsIndexes.EndStationCode])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrStationCodeType, dataErrors.ErrInvalidTripData)
	}

	yearID, err := strconv.Atoi(dataSplit[tb.config.ValidColumnsIndexes.YearID])
	if err != nil {
		log.Debugf("Invalid year ID: %v", dataSplit[tb.config.ValidColumnsIndexes.YearID])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidYearIDType, dataErrors.ErrInvalidTripData)
	}

	duration, err := strconv.ParseFloat(dataSplit[tb.config.ValidColumnsIndexes.Duration], 64)
	if err != nil {
		log.Debugf("Invalid duration type: %v", dataSplit[tb.config.ValidColumnsIndexes.Duration])
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
func (tb *TripBroker) isValid(tripData *trip.TripData) bool {
	return tripData.StartDate.Year() == tripData.YearID &&
		tripData.Duration > 0.0 &&
		tripData.StartStationCode > 0 &&
		tripData.EndStationCode > 0
}
