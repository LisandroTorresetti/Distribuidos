package station

import (
	"fmt"
	"strconv"
	"tp1/domain/entities"
)

const (
	latitudeBound  = 90
	longitudeBound = 180
)

// StationData struct that contains the station data
// + Metadata: metadata added to the structure
// + Code: ID of the station
// + Name: name of the station
// + Latitude: latitude of the station
// + Longitude: longitude of the station
// + YearID: year in which the station begins to operate
type StationData struct {
	Metadata  entities.Metadata `json:"metadata"`
	Code      int               `json:"code"`
	Name      string            `json:"name"`
	Latitude  string            `json:"latitude"`
	Longitude string            `json:"longitude"`
	YearID    int               `json:"year_id"`
}

func (sd StationData) GetMetadata() entities.Metadata {
	return sd.Metadata
}

// GetPrimaryKey returns the key to identify a station. The structure is: code-yearID
func (sd StationData) GetPrimaryKey() string {
	return fmt.Sprintf("%v-%v", sd.Code, sd.YearID)
}

// HasValidCoordinates returns if both Latitude and Longitude are valid
func (sd StationData) HasValidCoordinates() bool {
	if sd.Latitude == "" || sd.Longitude == "" {
		return false
	}

	latitude, longitude := sd.GetCoordinates()

	if !(-latitudeBound <= latitude && latitude <= latitudeBound) {
		return false
	}

	if !(-longitudeBound <= longitude && longitude <= longitudeBound) {
		return false
	}

	return true
}

// GetCoordinates returns the Latitude and Longitude as float64
func (sd StationData) GetCoordinates() (float64, float64) {
	latitude, err := strconv.ParseFloat(sd.Latitude, 64)
	if err != nil {
		panic(fmt.Sprintf("error converting latitude %s: %s", sd.Latitude, err.Error()))
	}

	longitude, err := strconv.ParseFloat(sd.Longitude, 64)
	if err != nil {
		panic(fmt.Sprintf("error converting longitude %s: %s", sd.Longitude, err.Error()))
	}

	return latitude, longitude
}

// NOT USED

// MultipleStations has the latitude and longitude of two stations. The name corresponds to te end station
type MultipleStations struct {
	Metadata              entities.Metadata
	StartStationID        int     `json:"start_station_id"`
	EndStationID          int     `json:"end_station_id"`
	EndStationName        string  `json:"end_station_name"`
	YearID                int     `json:"year_id"`
	StartStationLatitude  float64 `json:"start_station_latitude"`
	StartStationLongitude float64 `json:"start_station_longitude"`
	EndStationLatitude    float64 `json:"end_station_latitude"`
	EndStationLongitude   float64 `json:"end_station_longitude"`
}

func NewMultipleStations(startStation *StationData, endStation *StationData, yearID int) *MultipleStations {
	startLatitude, startLongitude := startStation.GetCoordinates()
	endLatitude, endLongitude := endStation.GetCoordinates()
	return &MultipleStations{
		StartStationID:        startStation.Code,
		EndStationID:          endStation.Code,
		EndStationName:        endStation.Name,
		YearID:                yearID,
		StartStationLatitude:  startLatitude,
		StartStationLongitude: startLongitude,
		EndStationLatitude:    endLatitude,
		EndStationLongitude:   endLongitude,
	}
}

func (ms *MultipleStations) GetMetadata() entities.Metadata {
	return ms.Metadata
}

// GetStartStationCoordinates returns the start station latitude and longitude
func (ms *MultipleStations) GetStartStationCoordinates() (float64, float64) {
	return ms.StartStationLatitude, ms.StartStationLongitude
}

// GetEndStationCoordinates returns the start station latitude and longitude
func (ms *MultipleStations) GetEndStationCoordinates() (float64, float64) {
	return ms.EndStationLatitude, ms.EndStationLongitude
}

func (ms *MultipleStations) GetEndStationName() string {
	return ms.EndStationName
}

// GetCombinedIDs returns a combination of both IDs with the following structure: startStationID-endStationID
func (ms *MultipleStations) GetCombinedIDs() string {
	return fmt.Sprintf("%v-%v", ms.StartStationID, ms.EndStationID)
}
