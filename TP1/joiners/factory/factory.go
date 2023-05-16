package factory

import (
	"fmt"
	"tp1/communication"
	"tp1/joiners/factory/joiner_type/cityjoiner"
	cityJoinerConfig "tp1/joiners/factory/joiner_type/cityjoiner/config"
	"tp1/joiners/factory/joiner_type/rainjoiner"
	rainJoinerConfig "tp1/joiners/factory/joiner_type/rainjoiner/config"
	"tp1/joiners/factory/joiner_type/yearjoiner"
	yearJoinerConfig "tp1/joiners/factory/joiner_type/yearjoiner/config"
)

var (
	rainJoinerType = "rain-joiner"
	yearJoinerType = "year-joiner"
	cityJoinerType = "city-joiner"
)

type Joiner interface {
	GetID() string
	GetType() string
	GetCity() string
	GetRoutingKeys() []string
	GetEOFString() string
	GetExpectedEOFString(data string) string
	DeclareQueues() error
	DeclareExchanges() error
	JoinData() error
	SendResult() error
	SendEOF() error
	Kill() error
}

func NewJoiner(joinerType string) (Joiner, error) {
	rabbitMQ, err := communication.NewRabbitMQ()
	if err != nil {
		return nil, err
	}

	if joinerType == rainJoinerType {
		cfg, err := rainJoinerConfig.LoadConfig()
		if err != nil {
			return nil, err
		}

		return rainjoiner.NewRainJoiner(rabbitMQ, cfg), nil
	}

	if joinerType == cityJoinerType {
		cfg, err := cityJoinerConfig.LoadConfig()
		if err != nil {
			return nil, err
		}

		return cityjoiner.NewCityJoiner(rabbitMQ, cfg), nil
	}

	if joinerType == yearJoinerType {
		cfg, err := yearJoinerConfig.LoadConfig()
		if err != nil {
			return nil, err
		}

		return yearjoiner.NewYearJoiner(rabbitMQ, cfg), nil
	}

	return nil, fmt.Errorf("[method: NewJoiner][status: error] Invalid joiner type %s", joinerType)
}
