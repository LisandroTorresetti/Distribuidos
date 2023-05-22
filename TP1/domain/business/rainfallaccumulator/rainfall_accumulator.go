package rainfallaccumulator

import "tp1/domain/entities"

// RainfallAccumulator struct that collects data about the total duration of rides in a given date with precipitations.
// + Metadata: metadata added to the structure
// + Counter: counts the amount of data collected
// + TotalDuration: sum of durations of rides
type RainfallAccumulator struct {
	Metadata      entities.Metadata `json:"metadata"`
	Counter       int               `json:"counter"`
	TotalDuration float64           `json:"total_duration"`
}

func NewRainfallAccumulator() *RainfallAccumulator {
	return &RainfallAccumulator{}
}

func (ra *RainfallAccumulator) UpdateAccumulator(duration float64) {
	ra.Counter += 1
	ra.TotalDuration += duration
}

func (ra *RainfallAccumulator) GetAverageDuration() float64 {
	if ra.Counter == 0 {
		return 0.0
	}
	return ra.TotalDuration / float64(ra.Counter)
}

func (ra *RainfallAccumulator) Merge(other *RainfallAccumulator) {
	ra.Counter += other.Counter
	ra.TotalDuration += other.TotalDuration
}

func (ra *RainfallAccumulator) GetMetadata() entities.Metadata {
	return ra.Metadata
}
