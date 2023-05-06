package trip

import "time"

// TripData struct that contains the weather field to analyze
type TripData struct {
	City             string    `json:"city"`
	Type             string    `json:"type"`
	StartDate        time.Time `json:"start_date"`
	StartStationCode int       `json:"start_station_code"`
	EndDate          time.Time `json:"end_date"`
	EndStationCode   int       `json:"end_station_code"`
	Duration         float64   `json:"duration"`
	YearID           int       `json:"year_id"`
}
