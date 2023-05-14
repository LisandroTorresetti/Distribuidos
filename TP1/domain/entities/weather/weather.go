package weather

import "time"

// WeatherData struct that contains the data of weather:
// + City: city which belongs the data
// + Type: this field helps us to recognize in different stages what type of data is, in this case the value will always be 'weather'
// + Date: date of the data (with the corresponding operation perform to get the true value due to the date is ahead by one day)
// + Rainfall: precipitations related with Date attribute.
type WeatherData struct {
	City     string    `json:"city"`
	Type     string    `json:"type"`
	Date     time.Time `json:"date"`
	Rainfall float64   `json:"rainfall"`
}
