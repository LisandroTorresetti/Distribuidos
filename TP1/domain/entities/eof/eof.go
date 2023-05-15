package eof

import "tp1/domain/entities"

const eofType = "EOF"

// EOFData struct that it's send by the EOF Manager. Has two attributes that are in all domain entities.
// + Metadata: metadata added to the structure
type EOFData struct {
	Metadata entities.Metadata `json:"metadata"`
}

func NewEOF(city string, stage string, eofMessage string) *EOFData {
	return &EOFData{
		Metadata: entities.NewMetadata(city, eofType, stage, eofMessage),
	}
}

func (eof EOFData) GetMetadata() entities.Metadata {
	return eof.Metadata
}
