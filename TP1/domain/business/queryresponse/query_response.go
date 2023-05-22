package queryresponse

import "tp1/domain/entities"

// QueryResponse contains the response of a query
type QueryResponse struct {
	Metadata entities.Metadata `json:"metadata"`
	QueryID  string            `json:"query_id"`
}

func NewQueryResponse(queryID string, response string, sender string, responseType string) *QueryResponse {
	metadata := entities.NewMetadata("", responseType, sender, response)
	return &QueryResponse{
		Metadata: metadata,
		QueryID:  queryID,
	}
}

func (qr *QueryResponse) GetMetadata() entities.Metadata {
	return qr.Metadata
}

func (qr *QueryResponse) GetQueryID() string {
	return qr.QueryID
}
