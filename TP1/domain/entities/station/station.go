package station

// StationData struct that contains the station data
// + City: city which belongs the data
// + Type: this field helps us to recognize in different stages what type of data is, in this case the value will always be 'stations'
// + Code: ID of the station
// + Name: name of the station
// + Latitude: latitude of the station
// + Longitude: longitude of the station
// + YearID: year in which the station begins to operate
type StationData struct {
	City      string  `json:"city"`
	Type      string  `json:"type"`
	Code      int     `json:"code"`
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	YearID    int     `json:"year_id"`
}
