package factory

import (
	"fmt"
	"tp1/communication"
	"tp1/queryhandlers/factory/handler_type/rainhandler"
	"tp1/queryhandlers/factory/handler_type/rainhandler/config"
)

var (
	rainHandlerType = "rain-handler"
	//duplicatesHandler = "duplicates-handler"
	//cityHandler = "city-handler"
)

type Handler interface {
	GetQueryID() string
	GetType() string
	GetEOFString() string
	GetExpectedEOFString() string
	DeclareQueues() error
	GenerateResponse() error
	SendResponse() error
	SendEOF() error
	Kill() error
}

func NewQueryHandler(handlerType string) (Handler, error) {
	rabbitMQ, err := communication.NewRabbitMQ()
	if err != nil {
		return nil, err
	}

	if handlerType == rainHandlerType {
		cfg, err := config.LoadConfig()
		if err != nil {
			return nil, err
		}

		return rainhandler.NewRainHandler(rabbitMQ, cfg), nil
	}

	/*if handlerType == cityJoinerType {
		cfg, err := cityJoinerConfig.LoadConfig()
		if err != nil {
			return nil, err
		}

		return cityjoiner.NewCityJoiner(rabbitMQ, cfg), nil
	}

	if handlerType == yearJoinerType {
		cfg, err := yearJoinerConfig.LoadConfig()
		if err != nil {
			return nil, err
		}

		return yearjoiner.NewYearJoiner(rabbitMQ, cfg), nil
	}*/

	return nil, fmt.Errorf("[method: NewQueryHandler][status: error] Invalid handler type %s", handlerType)
}