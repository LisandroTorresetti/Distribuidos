package distanceaccumulator

import (
	"fmt"
	"strconv"
	"tp1/domain/entities"
)

// DistanceAccumulator struct that collects data about the distance traveled to a given station
// + Metadata: metadata added to the structure
// + StationName: name of the station to collect data. Once set, it cannot change
// + StationID: ID of the station
// + Counter: counts the amount of data collected
// + TotalDistance: sum of distances traveled for the station
type DistanceAccumulator struct {
	Metadata      entities.Metadata `json:"metadata"`
	StationName   string            `json:"station_name"`
	StationID     string            `json:"station_id"`
	Counter       int               `json:"counter"`
	TotalDistance float64           `json:"total_distance"`
}

func NewDistanceAccumulator(stationName string, stationID string) *DistanceAccumulator {
	return &DistanceAccumulator{
		StationName: stationName,
		StationID:   stationID,
	}
}

func NewDistanceAccumulatorWithData(metadata entities.Metadata, stationName string, stationID string, counter int, totalDistance float64) *DistanceAccumulator {
	return &DistanceAccumulator{
		Metadata:      metadata,
		StationName:   stationName,
		StationID:     stationID,
		Counter:       counter,
		TotalDistance: totalDistance,
	}
}

func (da *DistanceAccumulator) GetMetadata() entities.Metadata {
	return da.Metadata
}

func (da *DistanceAccumulator) UpdateAccumulator(newDistance float64) {
	da.Counter += 1
	da.TotalDistance += newDistance
}

func (da *DistanceAccumulator) Merge(distanceAccumulator2 *DistanceAccumulator) {
	da.Counter += distanceAccumulator2.Counter
	da.TotalDistance += distanceAccumulator2.TotalDistance
}

func (da *DistanceAccumulator) GetAverageDistance() float64 {
	if da.Counter == 0 {
		panic("[DistanceAccumulator] cannot get average, counter is zero")
	}
	return da.TotalDistance / float64(da.Counter)
}

func (da *DistanceAccumulator) GetIDAsInt() int {
	id, err := strconv.Atoi(da.StationID)
	if err != nil {
		panic(fmt.Sprintf("cannot convert string to int for DistanceAccumulator, %s", da.StationID))
	}
	return id
}
