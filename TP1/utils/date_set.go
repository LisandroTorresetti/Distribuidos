package utils

import "time"

type DateSet map[string]bool

func (ds DateSet) Add(element time.Time) {
	ds[element.String()] = true
}

func (ds DateSet) Contains(element time.Time) bool {
	return ds[element.String()]
}
