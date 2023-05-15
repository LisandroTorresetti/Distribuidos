package distanceaccumulator

import "tp1/domain/entities"

// DistanceAccumulator struct that collects data about the distance traveled to a given station
// + Metadata: metadata added to the structure
// + Name: name of the station to collect data. Once set, it cannot change
// + Counter: counts the amount of data collected
// + TotalDistance: sum of distances traveled for the station
type DistanceAccumulator struct {
	Metadata      entities.Metadata `json:"metadata"`
	Name          string            `json:"name"`
	Counter       int               `json:"counter"`
	TotalDistance float64           `json:"total_distance"`
}

func NewDistanceAccumulator(stationName string) *DistanceAccumulator {
	return &DistanceAccumulator{
		Name: stationName,
	}
}

func NewDistanceAccumulatorWithData(metadata entities.Metadata, stationName string, counter int, totalDistance float64) *DistanceAccumulator {
	return &DistanceAccumulator{
		Metadata:      metadata,
		Name:          stationName,
		Counter:       counter,
		TotalDistance: totalDistance,
	}
}

func (da *DistanceAccumulator) UpdateAccumulator(newDistance float64) {
	da.Counter += 1
	da.TotalDistance += newDistance
}

func (da *DistanceAccumulator) Merge(distanceAccumulator2 *DistanceAccumulator) *DistanceAccumulator {
	if da.Name != distanceAccumulator2.Name {
		panic("[DistanceAccumulator] cannot merge two DistanceAccumulator with different names")
	}
	mergedCounter := da.Counter + distanceAccumulator2.Counter
	mergedDistance := da.TotalDistance + distanceAccumulator2.TotalDistance

	return &DistanceAccumulator{
		Name:          da.Name,
		Counter:       mergedCounter,
		TotalDistance: mergedDistance,
	}
}

func (da *DistanceAccumulator) GetAverageDistance() float64 {
	if da.Counter == 0 {
		panic("[DistanceAccumulator] cannot get average, counter is zero")
	}
	return da.TotalDistance / float64(da.Counter)
}
