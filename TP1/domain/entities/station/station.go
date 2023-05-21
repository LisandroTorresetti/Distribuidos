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
