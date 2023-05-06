package main

type eofConfig struct {
	AmountOfWeatherFilters int
	AmountOfStationFilters int
	AmountOfTripFilters    int
}

type EOFManager struct {
	config eofConfig
}

func NewEOFManager(config eofConfig) *EOFManager {
	return &EOFManager{
		config: config,
	}
}

//func (eof *EOFManager) StartManaging()
