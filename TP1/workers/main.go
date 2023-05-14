package main

import (
	log "github.com/sirupsen/logrus"
	"os"
	"tp1/workers/factory"
)

const (
	logLevelEnv   = "LOG_LEVEL"
	workerTypeEnv = "WORKER_TYPE"
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
	logLevel := os.Getenv(logLevelEnv)
	if logLevel == "" {
		logLevel = "DEBUG"
	}
	if err := InitLogger(logLevel); err != nil {
		log.Fatalf("%s", err)
		return
	}

	workerType := os.Getenv(workerTypeEnv)

	worker, err := factory.NewWorker(workerType)
	if err != nil {
		log.Debugf("Error creating worker: %s", err.Error())
		return
	}

	defer func(worker factory.IWorker) {
		err := worker.Kill()
		if err != nil {
			log.Errorf("[worker: %s][workerID: %v] error killing worker: %s", worker.GetType(), worker.GetID(), err.Error())
		}
	}(worker)

	/*err = worker.DeclareQueues()
	if err != nil {
		log.Debugf("[worker: %s][workerID: %v] %s", worker.GetType(), worker.GetID(), err.Error())
		return
	}*/

	err = worker.DeclareExchanges()
	if err != nil {
		log.Debugf("[worker: %s][workerID: %v] %s", worker.GetType(), worker.GetID(), err.Error())
		return
	}

	err = worker.ProcessInputMessages()
	if err != nil {
		log.Debugf("[worker: %s][workerID: %v] error processing messages: %s", worker.GetType(), worker.GetID(), err.Error())
		return
	}

	log.Debugf("[worker: %s][workerID: %v] finish main.go", worker.GetType(), worker.GetID())
}
