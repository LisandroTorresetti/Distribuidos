package station

// StationData struct that contains the weather field to analyze
type StationData struct {
	City      string  `json:"city"`
	Type      string  `json:"type"`
	Code      int     `json:"code"`
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	YearID    int     `json:"year_id"`
}
