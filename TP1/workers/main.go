package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
	"os"
	"tp1/workers/factory"
)

const rabbitUrl = "amqp://guest:guest@rabbit:5672/"

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

	workerType := os.Getenv("WORKER_TYPE")

	worker, err := factory.NewWorker(workerType)
	if err != nil {
		log.Debugf("Error creating worker: %s", err.Error())
		return
	}

	conn, err := amqp.Dial(rabbitUrl)
	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error] failed to connect to RabbitMQ: %s", worker.GetType(), worker.GetID(), err.Error())
		return
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Errorf("[worker: %s][workerID: %v][status: error] failed to open a RabbitMQ Channel: %s", worker.GetType(), worker.GetID(), err.Error())
		return
	}

	defer ch.Close()

	err = worker.DeclareQueues(ch)
	if err != nil {
		log.Debugf("%s", err.Error())
		return
	}

	err = worker.DeclareExchanges(ch)
	if err != nil {
		log.Debugf("%s", err.Error())
		return
	}

	err = worker.ProcessInputMessages(ch)
	if err != nil {
		log.Debugf("Error processing messages: %s", err.Error())
		return
	}

	log.Debugf("[worker: %s][workerID: %v] finish main.go", worker.GetType(), worker.GetID())
}
