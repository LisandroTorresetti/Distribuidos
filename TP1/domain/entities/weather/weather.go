package weather

import (
	"time"
	"tp1/domain/entities"
)

// WeatherData struct that contains the data of weather:
// + Metadata: metadata added to the structure
// + Date: date of the data (with the corresponding operation perform to get the true value due to the date is ahead by one day)
// + Rainfall: precipitations related with Date attribute
// + EOF: when this field is set, all the others have a zero-value. This field it's to indicate that all weathers data was processed
type WeatherData struct {
	Metadata entities.Metadata `json:"metadata"`
	Date     time.Time         `json:"date"`
	Rainfall float64           `json:"rainfall"`
}

func (wd WeatherData) GetMetadata() entities.Metadata {
	return wd.Metadata
}
