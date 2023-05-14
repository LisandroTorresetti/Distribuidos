package station

import "tp1/domain/entities"

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
	Latitude  float64           `json:"latitude"`
	Longitude float64           `json:"longitude"`
	YearID    int               `json:"year_id"`
}

func (sd StationData) GetMetadata() entities.Metadata {
	return sd.Metadata
}
