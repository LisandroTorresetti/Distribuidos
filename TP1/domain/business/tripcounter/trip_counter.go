package tripcounter

import (
	"fmt"
	"tp1/domain/entities"
)

// TripCounter struct that counts the amount of trips that begins in StationName
// + Metadata: metadata added to the structure
// + StationName: name of the station to collect data. Once set, it cannot change
// + StationID: ID of the station. Once set, it cannot change
// + YearID: ID of the year. Stations are found by StationID and YearID. Once set, it cannot change
// + Counter: counts the amount of trips that begins in some station
type TripCounter struct {
	Metadata    entities.Metadata `json:"metadata"`
	StationName string            `json:"name"`
	StationID   int               `json:"station_id"`
	YearID      int               `json:"year_id"`
	Counter     int               `json:"counter"`
}

func NewTripCounter(stationName string, stationID int, yearID int) *TripCounter {
	return &TripCounter{
		StationName: stationName,
		StationID:   stationID,
		YearID:      yearID,
	}
}

func (tc *TripCounter) UpdateCounter() {
	tc.Counter += 1
}

func (tc *TripCounter) GetCounter() int {
	return tc.Counter
}

func (tc *TripCounter) GetKey() string {
	return fmt.Sprintf("%v-%v", tc.StationID, tc.YearID)
}

func (tc *TripCounter) Merge(tripCounter2 *TripCounter) *TripCounter {
	// sanity checks
	if tc.StationName != tripCounter2.StationName {
		panic("[TripCounter] cannot merge two TripCounters with different name")
	}

	if tc.StationID != tripCounter2.StationID {
		panic("[TripCounter] cannot merge two TripCounters with different station ID")
	}

	if tc.YearID != tripCounter2.YearID {
		panic("[TripCounter] cannot merge two TripCounters with different year ID")
	}

	return &TripCounter{
		StationName: tc.StationName,
		StationID:   tc.StationID,
		YearID:      tc.YearID,
		Counter:     tc.Counter + tripCounter2.Counter,
	}
}

func (tc *TripCounter) DuplicateValues(otherTripCounter *TripCounter) bool {
	// sanity check
	if tc.StationName != otherTripCounter.StationName {
		panic("[TripCounter] cannot compare two TripCounters with different name")
	}

	if tc.StationID != otherTripCounter.StationID {
		panic("[TripCounter] cannot compare two TripCounters with different station ID")
	}

	if tc.YearID == otherTripCounter.YearID {
		panic("[TripCounter] cannot compare a TripCounter with itself")
	}

	return tc.Counter >= 2*otherTripCounter.GetCounter()
}
