package rainjoiner

import "errors"

var (
	ErrExchangeNotFound         = errors.New("exchange not found")
	ErrUnmarshallingWeatherData = errors.New("unexpected error unmarshalling Weather Data")
	ErrUnmarshallingTripData    = errors.New("unexpected error unmarshalling Trip Data")
	ErrMarshallingSummary       = errors.New("unexpected error marshalling rainfall summary")
)
