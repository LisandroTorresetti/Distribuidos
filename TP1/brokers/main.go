package main

import (
	log "github.com/sirupsen/logrus"
	"os"
)

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

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "DEBUG"
	}
	if err := InitLogger(logLevel); err != nil {
		log.Fatalf("%s", err)
		return
	}

	brokerType := os.Getenv("BROKER_TYPE")

	broker, err := NewBroker(brokerType, ",") // csv delimiter
	if err != nil {
		log.Debugf("Error creating broker: %s", err.Error())
		return
	}

	err = broker.DeclareQueues()
	if err != nil {
		log.Debugf("Error declaring queues: %s", err.Error())
		return
	}

	err = broker.DeclareExchanges()
	if err != nil {
		log.Debugf("Error declaring exchanges: %s", err.Error())
		return
	}

	err = broker.ProcessInputMessages()
	if err != nil {
		log.Debugf("Error processing messages: %s", err.Error())
		return
	}

	log.Debug("BROKER finish main.go")
}
