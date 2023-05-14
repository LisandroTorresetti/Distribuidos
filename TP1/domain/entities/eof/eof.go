package eof

import "tp1/domain/entities"

// EOF struct that it's send by the EOF Manager. Has two attributes that are in all domain entities.
// + Metadata: metadata added to the structure
const eofType = "EOF"

type EOF struct {
	Metadata entities.Metadata `json:"metadata"`
}

func NewEOF(city string, eofMessage string) *EOF {
	return &EOF{
		Metadata: entities.NewMetadata(city, eofType, eofMessage),
	}
}

func (eof EOF) GetMetadata() entities.Metadata {
	return eof.Metadata
}
