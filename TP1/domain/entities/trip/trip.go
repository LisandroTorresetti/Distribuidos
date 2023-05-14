package trip

import (
	"time"
	"tp1/domain/entities"
)

// TripData struct that contains the trip data
// + Metadata: metadata added to the structure
// + StartDate: date in which the trip begins
// + StartStationCode: station ID in which the trip begins
// + EndDate: date in which the trip ends
// + EndStationCode: station ID in which the trip ends
// + Duration: duration of the trip
// + YearID: year of the trip
type TripData struct {
	Metadata         entities.Metadata `json:"metadata"`
	StartDate        time.Time         `json:"start_date"`
	StartStationCode int               `json:"start_station_code"`
	EndDate          time.Time         `json:"end_date"`
	EndStationCode   int               `json:"end_station_code"`
	Duration         float64           `json:"duration"`
	YearID           int               `json:"year_id"`
}

func (td TripData) GetMetadata() entities.Metadata {
	return td.Metadata
}
