package rainjoiner

// RainfallAccumulator structure used at stage of joining data from weather and trips
type RainfallAccumulator struct {
	Counter       int     `json:"counter"`
	TotalDuration float64 `json:"total_duration"`
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
