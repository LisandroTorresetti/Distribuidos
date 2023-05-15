package entities

// Metadata this struct will contain extra information about the data that travels in our system
// + City: city which belongs the data
// + Type: this field helps us to recognize in different stages what type of data is
// + Stage: stage were the Metadata was constructed
// + Message: message with extra information
type Metadata struct {
	City    string `json:"city"`
	Type    string `json:"type"`
	Stage   string `json:"stage"`
	Message string `json:"message"`
}

func NewMetadata(city string, dataType string, stage string, message string) Metadata {
	return Metadata{
		City:    city,
		Type:    dataType,
		Stage:   stage,
		Message: message,
	}
}

func (m Metadata) GetType() string {
	return m.Type
}

func (m Metadata) GetCity() string {
	return m.City
}

func (m Metadata) GetStage() string {
	return m.Stage
}

func (m Metadata) GetMessage() string {
	return m.Message
}
