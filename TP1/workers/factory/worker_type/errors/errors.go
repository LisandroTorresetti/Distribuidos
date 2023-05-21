package errors

import "errors"

var (
	ErrInvalidStationData  = errors.New("invalid station data")
	ErrInvalidTripData     = errors.New("invalid trip data")
	ErrInvalidWeatherData  = errors.New("invalid weather data")
	ErrStationCodeType     = errors.New("invalid station code type")
	ErrInvalidYearIDType   = errors.New("invalid year ID type")
	ErrInvalidDate         = errors.New("invalid date")
	ErrInvalidDurationType = errors.New("invalid duration type")
	ErrInvalidRainfallType = errors.New("invalid rainfall type")
)
