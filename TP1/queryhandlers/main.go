package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"syscall"
	"tp1/queryhandlers/factory"
	"tp1/utils"
)

const (
	logLevelEnv    = "LOG_LEVEL"
	handlerTypeEnv = "HANDLER_TYPE"
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

	handlerType := os.Getenv(handlerTypeEnv)

	queryHandler, err := factory.NewQueryHandler(handlerType)
	if err != nil {
		log.Debugf("Error creating query handler: %s", err.Error())
		return
	}

	defer func(queryHandler factory.Handler) {
		err := queryHandler.Kill()
		if err != nil {
			log.Error(getLogMessage(queryHandler, "error killing handler", err))
			return
		}

		log.Info(getLogMessage(queryHandler, "Query handler killed successfully!", nil))
	}(queryHandler)

	signalChannel := utils.GetSignalChannel()

	err = queryHandler.DeclareQueues()
	if err != nil {
		log.Debug(getLogMessage(queryHandler, "error declaring queues", err))
		return
	}

	/* Next steps:
	1. Generate Data
	2. Send Response to server
	3. Send EOF to EOF Manager
	*/

	go func() {
		err = queryHandler.GenerateResponse()
		if err != nil {
			log.Debug(getLogMessage(queryHandler, "error generating response", err))
			return
		}
		log.Info(getLogMessage(queryHandler, "response generated successfully", nil))

		err = queryHandler.SendResponse()
		if err != nil {
			log.Debug(getLogMessage(queryHandler, "error sending response", err))
			return
		}
		log.Info(getLogMessage(queryHandler, "response sent successfully", nil))

		err = queryHandler.SendEOF()
		if err != nil {
			log.Debug(getLogMessage(queryHandler, "error sending EOF", err))
			return
		}

		log.Debug(getLogMessage(queryHandler, "Finish main.go", nil))
		signalChannel <- syscall.SIGTERM
	}()

	<-signalChannel
}

func getLogMessage(queryHandler factory.Handler, message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[caller: main][queryID: %s][queryHandlerType: %s][status: ERROR] %s: %s", queryHandler.GetQueryID(), queryHandler.GetType(), message, err.Error())
	}
	return fmt.Sprintf("[caller: main][queryID: %s][queryHandlerType: %s][status: OK] %s", queryHandler.GetQueryID(), queryHandler.GetType(), message)
}
