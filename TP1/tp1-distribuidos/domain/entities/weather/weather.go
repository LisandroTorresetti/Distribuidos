package weather

import "time"

// WeatherData struct that contains the weather field to analyze
type WeatherData struct {
	Date     time.Time `json:"date"`
	Rainfall float64   `json:"rainfall"`
}
