package station

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	dataErrors "tp1/brokers/errors"
	"tp1/brokers/station/config"
	"tp1/domain/entities/station"
)

const (
	latitudeBound  = 90
	longitudeBound = 180
)

type StationBroker struct {
	config    *config.StationConfig
	delimiter string
}

func NewStationBroker(delimiter string, stationBrokerConfig *config.StationConfig) *StationBroker {
	return &StationBroker{
		delimiter: delimiter,
		config:    stationBrokerConfig,
	}
}

func (sb *StationBroker) ProcessData(data string) error {
	log.Infof("Processing STATION data: %s", data)
	tripData, err := sb.getStationData(data)
	if err != nil {
		if errors.Is(err, dataErrors.ErrInvalidStationData) {
			return nil
		}
		return err
	}

	if sb.isValid(tripData) {
		// ToDo: here we should send a message to rabbitMQ
		log.Infof("IS VALID %s", data)
		return nil
	}

	log.Infof("INVALID DATA %s", data)
	return nil
}

// getStationData returns a struct StationData
func (sb *StationBroker) getStationData(data string) (*station.StationData, error) {
	dataSplit := strings.Split(data, sb.delimiter)

	stationCode, err := strconv.Atoi(dataSplit[sb.config.ValidColumnsIndexes.Code])
	if err != nil {
		log.Debugf("Invalid station code ID: %v", dataSplit[sb.config.ValidColumnsIndexes.Code])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrStationCodeType, dataErrors.ErrInvalidStationData)
	}

	latitude, err := strconv.ParseFloat(dataSplit[sb.config.ValidColumnsIndexes.Latitude], 64)
	if err != nil {
		log.Debugf("Invalid latitude: %v", dataSplit[sb.config.ValidColumnsIndexes.Latitude])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidLatitude, dataErrors.ErrInvalidStationData)
	}

	longitude, err := strconv.ParseFloat(dataSplit[sb.config.ValidColumnsIndexes.Longitude], 64)
	if err != nil {
		log.Debugf("Invalid longitude: %v", dataSplit[sb.config.ValidColumnsIndexes.Longitude])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidLongitude, dataErrors.ErrInvalidStationData)
	}

	yearID, err := strconv.Atoi(dataSplit[sb.config.ValidColumnsIndexes.YearID])
	if err != nil {
		log.Debugf("Invalid year ID: %v", dataSplit[sb.config.ValidColumnsIndexes.YearID])
		return nil, fmt.Errorf("%s: %w", dataErrors.ErrInvalidYearIDType, dataErrors.ErrInvalidStationData)
	}

	return &station.StationData{
		Code:      stationCode,
		Name:      dataSplit[sb.config.ValidColumnsIndexes.Name],
		Latitude:  latitude,
		Longitude: longitude,
		YearID:    yearID,
	}, nil
}

// isValid returns true if the following conditions are met:
// + The station code is greater than 0
// + Latitude is between -90 and 90
// + Longitude is between -180 and 180
// + Name is not the empty string
func (sb *StationBroker) isValid(stationData *station.StationData) bool {
	latitude := stationData.Latitude
	longitude := stationData.Longitude
	return len(stationData.Name) > 0 &&
		-latitudeBound <= latitude && latitude <= latitudeBound &&
		-longitudeBound <= longitude && longitude <= longitudeBound &&
		stationData.Code > 0
}
