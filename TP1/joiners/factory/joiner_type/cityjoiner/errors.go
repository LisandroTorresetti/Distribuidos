package cityjoiner

import "errors"

var (
	ErrUnmarshallingStationData = errors.New("unexpected error unmarshalling Station Data")
	ErrUnmarshallingTripsData   = errors.New("unexpected error unmarshalling Trips Data")
)
