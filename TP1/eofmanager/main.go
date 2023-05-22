package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"os"
	"tp1/communication"
	"tp1/utils"
)

const configFilepath = "./config/config.yaml"

// InitLogger Receives the log level to be set in logrus as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	customFormatter := &log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   false,
	}
	log.SetFormatter(customFormatter)
	log.SetLevel(level)
	return nil
}

func initEOFConfig() (*eofConfig, error) {
	configFile, err := utils.GetConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var cfg eofConfig
	err = yaml.Unmarshal(configFile, &cfg)
	if err != nil {
		return nil, fmt.Errorf("error parsing EOF Manager config file: %s", err)
	}

	return &cfg, nil
}

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "DEBUG"
	}

	if err := InitLogger(logLevel); err != nil {
		log.Fatalf("%s", err)
		return
	}

	eofManagerConfig, err := initEOFConfig()
	if err != nil {
		log.Fatalf("%s", err)
		return
	}

	rabbitMQ, err := communication.NewRabbitMQ()
	if err != nil {
		log.Errorf("[EOF Manager] error getting RabbitMQ instance: %s", err.Error())
		return
	}

	defer func(rabbitMQ *communication.RabbitMQ) {
		err := rabbitMQ.KillBadBunny()
		if err != nil {
			log.Errorf("[EOF Manager] error killing RabbitMQ instance: %s", err.Error())
		}
	}(rabbitMQ)

	eofManager := NewEOFManager(eofManagerConfig, rabbitMQ)

	signalChannel := utils.GetSignalChannel()
	err = eofManager.DeclareQueues()
	if err != nil {
		log.Errorf("[EOF Manager] error declaring queues: %s", err.Error())
		return
	}

	err = eofManager.DeclareExchanges()
	if err != nil {
		log.Errorf("[EOF Manager] error declaring exchanges: %s", err.Error())
		return
	}

	go func() {
		eofManager.StartManaging()
	}()
	<-signalChannel
	log.Debug("[EOF Manager] finish main.go")
}
