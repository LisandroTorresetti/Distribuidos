package trip

import "time"

// TripData struct that contains the trip data
// + City: city which belongs the data
// + Type: this field helps us to recognize in different stages what type of data is, in this case the value will always be 'trips'
// + StartDate: date in which the trip begins
// + StartStationCode: station ID in which the trip begins
// + EndDate: date in which the trip ends
// + EndStationCode: station ID in which the trip ends
// + Duration: duration of the trip
// + YearID: year of the trip
// + EOF: when this field is set, all the others have a zero-value. This field it's to indicate that all trips data was processed
type TripData struct {
	City             string    `json:"city"`
	Type             string    `json:"type"`
	StartDate        time.Time `json:"start_date"`
	StartStationCode int       `json:"start_station_code"`
	EndDate          time.Time `json:"end_date"`
	EndStationCode   int       `json:"end_station_code"`
	Duration         float64   `json:"duration"`
	YearID           int       `json:"year_id"`
	EOF              string    `json:"eof"`
}
