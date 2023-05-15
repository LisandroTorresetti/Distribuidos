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

func (ra *RainfallAccumulator) SetDuration(duration float64) {
	ra.TotalDuration = duration
}

func (ra *RainfallAccumulator) SetCounter(counter int) {
	ra.Counter = counter
}

func (ra *RainfallAccumulator) GetAverageDuration() float64 {
	if ra.Counter == 0 {
		panic("[RainfallAccumulator] cannot get average duration, counter is zero")
	}
	return ra.TotalDuration / float64(ra.Counter)
}
